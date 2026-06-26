"""
ConfigMap-aware file watcher.

Kubernetes mounts a ConfigMap as a tree of symlinks:

    /etc/emulator/config.json  →  ..data/config.json
    /etc/emulator/..data       →  ..2026_05_19_AAA   (atomic swap)

When the ConfigMap changes, kubelet creates a new ..XXX directory and
atomically retargets ..data, then deletes the old directory. Two things
that break naive ConfigMap watchers:

  1. inotify watches on the file itself miss the change because the
     symlink target swaps without touching the file's inode. We watch
     the parent directory instead.

  2. If you call realpath() on the path at startup and cache it, your
     cached path points into the soon-to-be-deleted directory and every
     subsequent open() will FileNotFoundError. Keep the symlink path
     and let open() resolve it fresh each time.
"""

import json
import logging
import threading
import time
from typing import Callable

from watchdog.events import FileSystemEventHandler
from watchdog.observers import Observer

log = logging.getLogger(__name__)

ConfigCallback = Callable[[dict], None]


def _validate(payload: dict) -> None:
    for key in ("x", "cpu", "ram", "net"):
        if key not in payload:
            raise KeyError(key)
    for sub in ("cpu", "ram", "net"):
        if "a" not in payload[sub] or "b" not in payload[sub]:
            raise KeyError(f"{sub}.a/b")
    # Optional. configure() also tolerates a missing/malformed peers
    # field, but we reject obviously-wrong shapes here so the error
    # surfaces in the watcher log rather than buried later.
    peers = payload.get("peers")
    if peers is not None and not isinstance(peers, list):
        raise ValueError("peers must be a list of strings if present")
    peer_names = payload.get("peer_names")
    if peer_names is not None and not isinstance(peer_names, dict):
        raise ValueError("peer_names must be an object {ip: role} if present")


def _load_and_apply(path: str, callback: ConfigCallback) -> None:
    try:
        with open(path, "rb") as f:
            raw = f.read()
    except FileNotFoundError:
        log.warning("Config file %s missing — staying idle", path)
        return
    if not raw.strip():
        log.info("Config file %s empty — staying idle", path)
        return
    try:
        payload = json.loads(raw)
        _validate(payload)
    except (json.JSONDecodeError, KeyError, ValueError, TypeError) as e:
        log.error("Bad config in %s: %s", path, e)
        return
    try:
        callback(payload)
    except Exception:
        log.exception("configure callback failed")


class ConfigWatcher(FileSystemEventHandler):
    def __init__(self, path: str, callback: ConfigCallback):
        # Keep the symlink path so each open() follows the current ..data
        # target (NOT realpath() — that would freeze us to a deleted dir).
        self.path = path
        self.callback = callback
        self._lock = threading.Lock()
        self._timer: threading.Timer | None = None

    # Trailing-edge debounce: every filesystem event cancels any pending
    # timer and schedules a fresh one 2 s from now.  kubelet's ConfigMap
    # swap generates multiple events spread over ~1–2 s (create new
    # timestamped directory, write files, swap ..data symlink, delete old
    # directory).  The old leading-edge approach fired after the very
    # first event — before the symlink had actually been swapped — and
    # then fired again when the swap arrived, causing duplicate configures.
    # With a 2 s trailing window the callback fires exactly ONCE, after
    # the last filesystem event from a single ConfigMap patch has settled.
    _DEBOUNCE_S = 2.0

    def _maybe_reload(self) -> None:
        with self._lock:
            if self._timer is not None:
                self._timer.cancel()
            self._timer = threading.Timer(self._DEBOUNCE_S, self._do_reload)
            self._timer.daemon = True
            self._timer.start()

    def _do_reload(self) -> None:
        with self._lock:
            self._timer = None
        _load_and_apply(self.path, self.callback)

    def on_any_event(self, event) -> None:
        # Any event in the watch directory could be the ..data symlink swap
        # or a temp file landing inside the new ..XXX directory. We can't
        # filter on exact paths because kubelet uses unpredictable
        # timestamped names; the debounce collapses the burst into one call.
        self._maybe_reload()


def start_config_watcher(path: str, callback: ConfigCallback) -> Observer:
    import os
    watch_dir = os.path.dirname(path) or "."
    os.makedirs(watch_dir, exist_ok=True)
    handler = ConfigWatcher(path, callback)
    observer = Observer()
    observer.schedule(handler, watch_dir, recursive=False)
    observer.daemon = True
    observer.start()
    log.info("Watching %s for config changes", path)
    return observer


def load_initial(path: str, callback: ConfigCallback) -> None:
    """Apply whatever config is on disk right now — used at pod startup
    since the ConfigMap is already mounted before the container starts."""
    _load_and_apply(path, callback)
