import json
import os
import threading
from datetime import datetime, timezone


class PipelineController:
    """
    Persistent execution controller for BE-2.

    State is stored on disk so a process restart does not lose
    pause/checkpoint information.
    """

    def __init__(self, state_dir=".pipeline_state"):
        self.state_dir = state_dir
        os.makedirs(self.state_dir, exist_ok=True)

        self._lock = threading.RLock()
        self._pause_events = {}

    def _path(self, run_id):
        safe_id = str(run_id).replace("/", "_")
        return os.path.join(self.state_dir, f"{safe_id}.json")

    def _load(self, run_id):
        path = self._path(run_id)

        if not os.path.exists(path):
            return {
                "run_id": run_id,
                "status": "pending",
                "resources": {},
                "created_at": self._now(),
                "updated_at": self._now(),
            }

        with open(path, "r", encoding="utf-8") as f:
            return json.load(f)

    def _save(self, run_id, state):
        state["updated_at"] = self._now()

        path = self._path(run_id)
        tmp = f"{path}.tmp"

        with open(tmp, "w", encoding="utf-8") as f:
            json.dump(state, f, indent=2)

        os.replace(tmp, path)

    @staticmethod
    def _now():
        return datetime.now(timezone.utc).isoformat()

    def create_run(self, run_id, resources):
        with self._lock:
            state = self._load(run_id)

            state["status"] = "running"

            for resource in resources:
                state["resources"].setdefault(
                    resource,
                    {
                        "status": "pending",
                        "page": 0,
                        "offset": 0,
                        "cursor": None,
                        "records": 0,
                    },
                )

            self._save(run_id, state)

            self._pause_events.setdefault(
                run_id,
                threading.Event(),
            )

            self._pause_events[run_id].clear()

            return state

    def checkpoint(
        self,
        run_id,
        resource,
        page,
        offset,
        cursor,
        records,
    ):
        with self._lock:
            state = self._load(run_id)

            state["resources"][resource] = {
                "status": "running",
                "page": page,
                "offset": offset,
                "cursor": cursor,
                "records": records,
            }

            self._save(run_id, state)

            print(
                f"[CHECKPOINT] "
                f"{resource} page={page} "
                f"offset={offset} cursor={cursor}"
            )

    def pause(self, run_id):
        with self._lock:
            event = self._pause_events.setdefault(
                run_id,
                threading.Event(),
            )

            event.set()

            state = self._load(run_id)
            state["status"] = "paused"
            self._save(run_id, state)

    def resume(self, run_id):
        with self._lock:
            event = self._pause_events.setdefault(
                run_id,
                threading.Event(),
            )

            event.clear()

            state = self._load(run_id)
            state["status"] = "running"
            self._save(run_id, state)

    def is_paused(self, run_id):
        with self._lock:
            event = self._pause_events.setdefault(
                run_id,
                threading.Event(),
            )

            return event.is_set()

    def get_state(self, run_id):
        with self._lock:
            return self._load(run_id)

    def mark_completed(self, run_id):
        with self._lock:
            state = self._load(run_id)
            state["status"] = "completed"
            self._save(run_id, state)

    def mark_failed(self, run_id, error):
        with self._lock:
            state = self._load(run_id)
            state["status"] = "failed"
            state["error"] = str(error)
            self._save(run_id, state)
