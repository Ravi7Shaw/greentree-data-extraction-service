from __future__ import annotations

from datetime import datetime, timezone
from threading import Lock
from typing import Any

from services.pipeline.be2_controller import BE2PipelineController
from services.pipeline.hubspot_pipeline import HubSpotDealsPipeline


class BE2ScanService:
    """
    Thin orchestration layer between the Flask API and the BE-2 pipeline.

    Keeps scan state in the existing BE-2 controller while delegating
    extraction/storage to HubSpotDealsPipeline.
    """

    def __init__(self):
        self.controller = BE2PipelineController()
        self._lock = Lock()
        self._pipelines: dict[str, HubSpotDealsPipeline] = {}

    def create(self, scan_id: str, tenant_id: str, token: str, filters: dict[str, Any]):
        with self._lock:
            self.controller.create_run(
                scan_id,
                resources=["deals", "companies", "contacts"],
            )

            self._pipelines[scan_id] = HubSpotDealsPipeline(
                access_token=token,
                tenant_id=tenant_id,
                scan_id=scan_id,
                filters=filters,
                controller=self.controller,
            )

        return {
            "scanId": scan_id,
            "status": "pending",
            "createdAt": datetime.now(timezone.utc).isoformat(),
        }

    def run(self, scan_id: str):
        pipeline = self._pipelines.get(scan_id)

        if pipeline is None:
            raise ValueError(f"BE-2 pipeline not found for scan {scan_id}")

        self.controller.start(scan_id)

        try:
            result = pipeline.run()
            self.controller.complete(scan_id)
            return result
        except Exception as exc:
            self.controller.fail(scan_id, str(exc))
            raise

    def pause(self, scan_id: str):
        self.controller.pause(scan_id)

    def resume(self, scan_id: str):
        self.controller.resume(scan_id)

    def state(self, scan_id: str):
        return self.controller.get_state(scan_id)

    def remove(self, scan_id: str):
        with self._lock:
            self._pipelines.pop(scan_id, None)
