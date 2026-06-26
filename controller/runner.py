"""
Scenario runner — advances the system x through runtime_scenarios on a clock.

Started when a topology with runtime_scenarios is POSTed; stopped on DELETE.
Every tick it finds the active phase by elapsed minutes and, when the phase
changes, calls materialiser.patch_system_x so every role's ConfigMap picks up
the new resolved x. Workers hot-reload within ~2 s via their file watcher —
no pod restart needed.

One runner runs at a time. start() replaces any existing runner; stop() shuts
it down. Both are safe to call from the FastAPI threadpool.
"""

import logging
import threading
import time

import materialiser

log = logging.getLogger(__name__)

TICK_INTERVAL = 5.0  # seconds between phase checks

_lock = threading.Lock()  # protects _current
_current: "ScenarioRunner | None" = None


class ScenarioRunner:
    def __init__(self, phases: list[dict], template_name: str) -> None:
        self._phases = phases
        self._name = template_name
        self._t0 = time.time()
        self._stop_event = threading.Event()
        self._thread = threading.Thread(
            target=self._run, daemon=True,
            name=f"scenario-runner-{template_name}")
        self._last_phase_id: str | None = None

    def start(self) -> None:
        self._thread.start()
        log.info("runner: started for %r (%d phase(s), span=%.0f min)",
                 self._name, len(self._phases),
                 self._phases[-1]["end_min"] if self._phases else 0)

    def stop(self) -> None:
        self._stop_event.set()
        self._thread.join(timeout=TICK_INTERVAL + 2)
        log.info("runner: stopped for %r", self._name)

    def _active_phase(self, elapsed_min: float) -> dict | None:
        for ph in self._phases:
            if ph["start_min"] <= elapsed_min < ph["end_min"]:
                return ph
        return None

    def _run(self) -> None:
        _logged_complete = False
        # wait(timeout) blocks until the event is set or the timeout expires;
        # returns True (stop requested) or False (tick).
        while not self._stop_event.wait(timeout=TICK_INTERVAL):
            elapsed_min = (time.time() - self._t0) / 60.0
            ph = self._active_phase(elapsed_min)
            if ph is None:
                if self._phases and not _logged_complete:
                    last = self._phases[-1]
                    if elapsed_min >= last["end_min"]:
                        log.info("runner: scenario complete for %r — "
                                 "holding final x=%s", self._name, last["x"])
                        _logged_complete = True
                continue

            phase_id = ph.get("phase_id") or str(ph["start_min"])
            if phase_id == self._last_phase_id:
                continue  # still in the same phase

            log.info("runner: entering phase %r x=%s (elapsed=%.1f min)",
                     phase_id, ph["x"], elapsed_min)
            try:
                materialiser.patch_system_x(self._name, ph["x"])
                self._last_phase_id = phase_id
            except Exception as exc:
                log.warning("runner: patch_system_x failed (%s) — will retry", exc)


def start(doc: dict, template_name: str) -> None:
    """Start a runner for `doc`. Replaces any existing runner. No-op if the
    doc has no runtime_scenarios."""
    global _current
    phases = materialiser.scenario_x_timeline(doc)
    if not phases:
        return
    with _lock:
        if _current is not None:
            _current.stop()
        r = ScenarioRunner(phases, template_name)
        r.start()
        _current = r


def stop() -> None:
    """Stop the running runner, if any."""
    global _current
    with _lock:
        if _current is not None:
            _current.stop()
            _current = None
