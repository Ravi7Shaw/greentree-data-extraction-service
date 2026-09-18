import logging

import dlt

from .data_source import create_deals_resource
from .hubspot_api_service import HubSpotDealsAPIService
from config import Config

log = logging.getLogger(__name__)


class ExtractionService:
    def __init__(self):
        self.api = HubSpotDealsAPIService(
            Config.HUBSPOT_API_BASE_URL,
            Config.HUBSPOT_API_TIMEOUT,
            Config.HUBSPOT_RETRY_ATTEMPTS,
            Config.HUBSPOT_RETRY_DELAY,
        )

    def run(
        self,
        access_token,
        scan_id,
        tenant_id,
        limit=100,
        resume_after=None,
        checkpoint_callback=None,
    ):
        if not self.api.validate_credentials(access_token):
            raise ValueError("HubSpot credentials validation failed")

        limit = min(max(int(limit), 1), 100)

        resource = create_deals_resource(
            self.api,
            access_token,
            scan_id,
            tenant_id,
            limit=limit,
            checkpoint_callback=checkpoint_callback,
            resume_after=resume_after,
        )

        pipeline = dlt.pipeline(
            pipeline_name=Config.DLT_PIPELINE_NAME,
            destination=dlt.destinations.postgres(credentials=Config.db_url()),
            dataset_name=Config.DB_SCHEMA,
        )

        info = pipeline.run(resource)

        return {
            "pipeline": pipeline.pipeline_name,
            "load_id": getattr(info, "load_id", None),
            "dataset": Config.DB_SCHEMA,
            "limit": limit,
        }
