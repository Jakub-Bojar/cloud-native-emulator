"""
Declarative ingestion: ConfigMap reconciler.

Users can write a Kubernetes ConfigMap with the label
`emulator.local/template=true` and a `data.template.json` field
containing a topology (same schema as the HTTP POST body). This module
polls those labelled ConfigMaps every WATCH_INTERVAL_S seconds and
keeps the cluster in sync:

  - A labelled CM appears → materialize it.
  - Its template.json content changes → re-materialize.
  - The labelled CM is deleted → tear the template down.

The reconciler only touches templates created via this path (the
materializer stamps each one with `source=watch`). Templates created
via POST /templates are untouched even if no labelled CM names them.

The template name is taken from the labelled ConfigMap's
metadata.name. Any `name` field inside data.template.json is
overwritten — the Kubernetes object name is canonical, and trying to
honour both would create a name-mismatch edge case for no benefit.

Poll-based rather than watch-based: the Kubernetes streaming watch
endpoint needs resourceVersion bookkeeping and reconnects on hangup.
A 10s GET loop is simpler and good enough for a prototype.
"""

import json
import logging
import os
import threading
import time

import k8s
import materializer

log = logging.getLogger(__name__)

# Label on the user-authored ConfigMap that flags it as a template
# declaration. Both key and value are matched exactly.
TEMPLATE_LABEL_KEY = "emulator.local/template"
TEMPLATE_LABEL_VALUE = "true"

# Field inside data.* that carries the template JSON.
TEMPLATE_DATA_KEY = "template.json"

WATCH_INTERVAL_S = float(os.environ.get("WATCH_INTERVAL_S", "10"))


def _list_labelled_templates() -> dict[str, dict]:
    """Return {template_name: parsed_template_dict} for every labelled
    ConfigMap currently in the namespace. The template's `name` field is
    forced to the ConfigMap's metadata.name."""
    selector = f"{TEMPLATE_LABEL_KEY}={TEMPLATE_LABEL_VALUE}"
    from urllib.parse import quote
    path = (f"/api/v1/namespaces/{k8s.namespace()}/configmaps"
            f"?labelSelector={quote(selector)}")
    status, body = k8s.get(path)
    if status != 200:
        log.warning("list labelled ConfigMaps: status=%s", status)
        return {}

    out: dict[str, dict] = {}
    for cm in json.loads(body).get("items", []):
        meta = cm.get("metadata", {})
        name = meta.get("name")
        raw = (cm.get("data") or {}).get(TEMPLATE_DATA_KEY)
        if not name or not raw:
            log.warning("labelled ConfigMap %s has no %s field; ignoring",
                        name, TEMPLATE_DATA_KEY)
            continue
        try:
            template = json.loads(raw)
        except json.JSONDecodeError as e:
            log.warning("labelled ConfigMap %s: template.json is not "
                        "valid JSON: %s", name, e)
            continue
        if not isinstance(template, dict):
            log.warning("labelled ConfigMap %s: template.json is not an "
                        "object", name)
            continue
        # The CM's metadata.name is canonical; overwrite any name in the
        # JSON so we have one identifier the whole pipeline agrees on.
        template["name"] = name
        out[name] = template
    return out


def _templates_equal(a: dict, b: dict) -> bool:
    """Deep-equality via canonical JSON. Avoids tripping on dict-key
    ordering or float-vs-int differences that arise during round-trip."""
    return (json.dumps(a, sort_keys=True, separators=(",", ":"))
            == json.dumps(b, sort_keys=True, separators=(",", ":")))


def reconcile_once() -> None:
    """One pass: bring the cluster into agreement with labelled CMs."""
    expected = _list_labelled_templates()
    existing = materializer.list_managed_with_source(materializer.SOURCE_WATCH)

    # Create + update.
    for name, template in expected.items():
        try:
            materializer.validate(template)
        except ValueError as e:
            log.warning("labelled CM %s: invalid template: %s", name, e)
            continue
        if name in existing and _templates_equal(template, existing[name]):
            continue  # no drift
        action = "update" if name in existing else "create"
        log.info("reconcile %s: %s", name, action)
        try:
            materializer.materialize(template, source=materializer.SOURCE_WATCH)
        except Exception:
            log.exception("materialize from watch failed for %s", name)

    # Delete: anything we previously created from watch that no longer
    # has a labelled CM.
    for name in existing.keys() - expected.keys():
        log.info("reconcile %s: delete (labelled CM gone)", name)
        try:
            materializer.teardown(name)
        except Exception:
            log.exception("teardown from watch failed for %s", name)


def poll_loop() -> None:
    """Daemon entry point. Runs forever; logs and continues on errors."""
    log.info("ConfigMap watcher: polling every %.1fs for labelled "
             "ConfigMaps (%s=%s)", WATCH_INTERVAL_S,
             TEMPLATE_LABEL_KEY, TEMPLATE_LABEL_VALUE)
    while True:
        try:
            reconcile_once()
        except Exception:
            # An uncaught exception in a daemon thread silently kills it
            # and the controller carries on serving HTTP, hiding the bug.
            # Catch broadly, log, and continue.
            log.exception("reconcile_once raised")
        time.sleep(WATCH_INTERVAL_S)


def start() -> threading.Thread:
    t = threading.Thread(target=poll_loop, name="cm-watcher", daemon=True)
    t.start()
    return t
