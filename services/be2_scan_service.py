"""Single process supervisor; independent resources execute in worker threads."""

import fcntl
import logging
from concurrent.futures import ThreadPoolExecutor
from threading import Event, RLock

from config import Config
from services.pipeline.be2_controller import BE2PipelineController
from services.pipeline.hubspot_pipeline import HubSpotBE2Pipeline
from services.storage_service import StorageService

log = logging.getLogger(__name__)


class BE2ScanService:
    def __init__(self, controller=None, storage=None, pipeline=None):
        self.controller = controller or BE2PipelineController(
            Config.STATE_DIR, Config.TOKEN_ENCRYPTION_KEY
        )
        self.storage = storage or StorageService(self.controller.root)
        self.pipeline = pipeline or HubSpotBE2Pipeline(self.controller, self.storage)
        self.stopping = Event()
        self.lock = RLock()
        self.futures = {}
        self.executor = ThreadPoolExecutor(max_workers=Config.MAX_CONCURRENT_SCANS)
        self.process_lock = None

    def start(self):
        self.process_lock = open(self.controller.root / "worker.lock", "a")
        try:
            fcntl.flock(self.process_lock, fcntl.LOCK_EX | fcntl.LOCK_NB)
        except BlockingIOError:
            self.process_lock.close()
            raise RuntimeError("Only one API worker may own a STATE_DIR") from None
        for state in self.controller.list():
            if state["status"] in ("pausing", "cancelling"):
                self.controller.finish(state["scanId"])
            elif state["status"] in ("pending", "running"):
                self.submit(state["scanId"])

    def submit(self, scan_id):
        with self.lock:
            previous = self.futures.get(scan_id)
            if previous and not previous.done():
                return
            self.futures[scan_id] = self.executor.submit(self.run, scan_id)

    def run(self, scan_id):
        try:
            with self.lock:
                state = self.controller.get(scan_id)
                if state["status"] == "pending":
                    self.controller.transition(scan_id, {"pending"}, "running")
            self.pipeline.run(scan_id, self.stopping)
        except Exception:
            # Do not persist provider response bodies or credentials in API errors.
            log.error("Scan %s failed; inspect provider/storage availability", scan_id)

            def fail(state):
                if state["status"] in ("pausing", "cancelling"):
                    state["status"] = (
                        "paused" if state["status"] == "pausing" else "cancelled"
                    )
                else:
                    state["status"] = "failed"
                state["error"] = (
                    "Ingestion failed; resume retries the durable pending page"
                )

            self.controller.mutate(scan_id, fail)

    def create(self, scan_id, tenant, token, resources, limit):
        with self.lock:
            state, duplicate = self.controller.create(
                scan_id, tenant, token, resources, limit
            )
            if not duplicate:
                self.submit(scan_id)
            return state, duplicate

    def control(self, scan_id, action):
        with self.lock:
            if action == "pause":
                return self.controller.transition(
                    scan_id, {"pending", "running"}, "pausing"
                )
            if action == "cancel":
                state = self.controller.transition(
                    scan_id,
                    {"pending", "running", "pausing", "paused", "failed"},
                    "cancelling",
                )
                future = self.futures.get(scan_id)
                if not future or future.done():
                    state = self.controller.finish(scan_id)
                return state
            # A previous worker must fully exit before resume schedules its successor.
            future = self.futures.get(scan_id)
            if future and not future.done():
                from services.pipeline.be2_controller import ScanConflict

                raise ScanConflict(
                    "Worker is still stopping; retry resume after it exits"
                )
            state = self.controller.transition(scan_id, {"paused", "failed"}, "pending")
            self.submit(scan_id)
            return state

    def close(self):
        self.stopping.set()
        self.executor.shutdown(wait=True)
        if self.process_lock:
            self.process_lock.close()
