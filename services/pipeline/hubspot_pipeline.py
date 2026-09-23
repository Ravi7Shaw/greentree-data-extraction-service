import json
import logging
import os
import tempfile
from concurrent.futures import ThreadPoolExecutor, as_completed
from datetime import datetime, timezone

import clickhouse_connect
import dlt
from minio import Minio

from config import Config
from services.hubspot_api_service import HubSpotDealsAPIService

log = logging.getLogger(__name__)


HUBSPOT_RESOURCES = {
    "deals": {
        "endpoint": "/crm/v3/objects/deals",
        "properties": [
            "dealname",
            "amount",
            "dealstage",
            "pipeline",
            "closedate",
            "createdate",
            "hs_lastmodifieddate",
            "dealtype",
            "description",
        ],
    },
    "contacts": {
        "endpoint": "/crm/v3/objects/contacts",
        "properties": [
            "firstname",
            "lastname",
            "email",
            "phone",
            "company",
            "createdate",
            "lastmodifieddate",
        ],
    },
    "companies": {
        "endpoint": "/crm/v3/objects/companies",
        "properties": [
            "name",
            "domain",
            "industry",
            "phone",
            "city",
            "state",
            "country",
            "createdate",
            "hs_lastmodifieddate",
        ],
    },
    "tickets": {
        "endpoint": "/crm/v3/objects/tickets",
        "properties": [
            "content",
            "createdate",
            "hs_pipeline",
            "hs_pipeline_stage",
            "hs_ticket_category",
            "hs_ticket_priority",
        ],
    },
    "line_items": {
        "endpoint": "/crm/v3/objects/line_items",
        "properties": [
            "name",
            "quantity",
            "price",
            "amount",
            "createdate",
            "hs_product_id",
        ],
    },
    "engagements": {
        "endpoint": "/crm/v3/objects/engagements",
        "properties": [],
    },
    "pipelines": {
        "endpoint": "/crm/v3/pipelines/deals",
        "properties": [],
    },
    "owners": {
        "endpoint": "/crm/v3/owners",
        "properties": [],
    },
}


class PipelinePaused(Exception):
    pass


class HubSpotResourceClient(HubSpotDealsAPIService):
    def iter_resource(
        self,
        token,
        resource_name,
        limit=100,
        after=None,
    ):
        resource = HUBSPOT_RESOURCES[resource_name]

        cursor = after
        page = 0

        while True:
            params = {
                "limit": min(max(int(limit), 1), 100),
            }

            properties = resource.get("properties") or []

            if properties:
                params["properties"] = ",".join(properties)

            if cursor:
                params["after"] = cursor

            url = self.base_url + resource["endpoint"]

            data = self._request(
                token,
                url,
                params=params,
            )

            page += 1

            yield page, data

            cursor = (
                (data.get("paging") or {})
                .get("next", {})
                .get("after")
            )

            if not cursor:
                break


def transform_record(
    resource_name,
    record,
    scan_id,
    tenant_id,
):
    properties = record.get("properties") or {}

    return {
        "id": str(record.get("id", "")),
        "resource": resource_name,
        "properties": json.dumps(
            properties,
            separators=(",", ":"),
        ),
        "archived": bool(record.get("archived", False)),
        "_scan_id": scan_id,
        "_tenant_id": tenant_id,
        "_extracted_at": datetime.now(
            timezone.utc
        ).isoformat(),
    }


def create_resource(
    api,
    token,
    resource_name,
    scan_id,
    tenant_id,
    limit=100,
    resume_after=None,
    checkpoint_callback=None,
    pause_callback=None,
):
    @dlt.resource(
        name=resource_name,
        write_disposition="merge",
        primary_key="id",
    )
    def resource():
        for page, data in api.iter_resource(
            token,
            resource_name,
            limit=limit,
            after=resume_after,
        ):
            if pause_callback and pause_callback():
                raise PipelinePaused(
                    f"Pipeline paused at {resource_name} page {page}"
                )

            records = data.get("results", [])

            for record in records:
                yield transform_record(
                    resource_name,
                    record,
                    scan_id,
                    tenant_id,
                )

            next_cursor = (
                (data.get("paging") or {})
                .get("next", {})
                .get("after")
            )

            if checkpoint_callback:
                checkpoint_callback(
                    resource_name,
                    page,
                    next_cursor,
                    len(records),
                )

    return resource


class HubSpotBE2Pipeline:

    def __init__(self):
        self.api = HubSpotResourceClient(
            Config.HUBSPOT_API_BASE_URL,
            Config.HUBSPOT_API_TIMEOUT,
            Config.HUBSPOT_RETRY_ATTEMPTS,
            Config.HUBSPOT_RETRY_DELAY,
        )

    def run_resource(
        self,
        token,
        resource_name,
        scan_id,
        tenant_id,
        limit=100,
        resume_after=None,
        checkpoint_callback=None,
        pause_callback=None,
    ):
        log.info(
            "Starting resource %s for scan %s",
            resource_name,
            scan_id,
        )

        resource = create_resource(
            self.api,
            token,
            resource_name,
            scan_id,
            tenant_id,
            limit=limit,
            resume_after=resume_after,
            checkpoint_callback=checkpoint_callback,
            pause_callback=pause_callback,
        )

        pipeline = dlt.pipeline(
            pipeline_name=f"{Config.DLT_PIPELINE_NAME}_{resource_name}",
            destination=self._filesystem_destination(),
            dataset_name="hubspot",
            loader_file_format="parquet",
        )

        info = pipeline.run(resource)

        log.info(
            "Finished resource %s: %s",
            resource_name,
            info,
        )

        return {
            "resource": resource_name,
            "load_id": getattr(info, "load_id", None),
        }

    def run_parallel(
        self,
        token,
        resources,
        scan_id,
        tenant_id,
        limit=100,
        checkpoint_callback=None,
        pause_callback=None,
    ):
        results = []

        workers = min(
            len(resources),
            Config.MAX_CONCURRENT_SCANS,
        )

        with ThreadPoolExecutor(
            max_workers=workers
        ) as executor:

            futures = {
                executor.submit(
                    self.run_resource,
                    token,
                    resource_name,
                    scan_id,
                    tenant_id,
                    limit,
                    None,
                    checkpoint_callback,
                    pause_callback,
                ): resource_name
                for resource_name in resources
            }

            for future in as_completed(futures):
                resource_name = futures[future]

                try:
                    results.append(
                        future.result()
                    )
                except Exception:
                    log.exception(
                        "Resource %s failed",
                        resource_name,
                    )
                    raise

        return results

    @staticmethod
    def _filesystem_destination():
        credentials = {
            "aws_access_key_id": Config.MINIO_ACCESS_KEY,
            "aws_secret_access_key": Config.MINIO_SECRET_KEY,
            "endpoint_url": Config.MINIO_ENDPOINT,
            "region_name": Config.MINIO_REGION,
        }

        return dlt.destinations.filesystem(
            bucket_url=(
                f"s3://{Config.MINIO_BUCKET}"
            ),
            credentials=credentials,
            layout=(
                "{table_name}/"
                "year={YYYY}/"
                "month={MM}/"
                "{load_id}.{file_id}.{ext}"
            ),
        )


def ensure_minio_bucket():
    endpoint = Config.MINIO_ENDPOINT.replace(
        "http://", ""
    ).replace("https://", "")

    client = Minio(
        endpoint,
        access_key=Config.MINIO_ACCESS_KEY,
        secret_key=Config.MINIO_SECRET_KEY,
        secure=Config.MINIO_ENDPOINT.startswith("https"),
    )

    if not client.bucket_exists(
        Config.MINIO_BUCKET
    ):
        client.make_bucket(
            Config.MINIO_BUCKET
        )


def clickhouse_client():
    return clickhouse_connect.get_client(
        host=Config.CLICKHOUSE_HOST,
        port=Config.CLICKHOUSE_PORT,
        username=Config.CLICKHOUSE_USER,
        password=Config.CLICKHOUSE_PASSWORD or None,
        database=Config.CLICKHOUSE_DATABASE,
    )


def initialize_clickhouse():
    client = clickhouse_client()

    client.command(
        f"""
        CREATE DATABASE IF NOT EXISTS
        {Config.CLICKHOUSE_DATABASE}
        """
    )

    client.command(
        """
        CREATE TABLE IF NOT EXISTS hubspot_records
        (
            id String,
            resource LowCardinality(String),
            properties String,
            archived Bool,
            scan_id String,
            tenant_id String,
            extracted_at DateTime64(3)
        )
        ENGINE = ReplacingMergeTree(extracted_at)
        ORDER BY (resource, id)
        """
    )

    client.command(
        """
        CREATE OR REPLACE VIEW
        v_hubspot_deal_pipeline AS

        SELECT
            d.id AS deal_id,

            JSONExtractString(
                d.properties,
                'dealname'
            ) AS deal_name,

            JSONExtractFloat(
                d.properties,
                'amount'
            ) AS amount,

            JSONExtractString(
                d.properties,
                'dealstage'
            ) AS deal_stage,

            JSONExtractString(
                d.properties,
                'pipeline'
            ) AS pipeline_id,

            c.id AS company_id,

            JSONExtractString(
                c.properties,
                'name'
            ) AS company_name,

            JSONExtractString(
                c.properties,
                'domain'
            ) AS company_domain

        FROM hubspot_records d

        LEFT JOIN hubspot_records c
            ON JSONExtractString(
                d.properties,
                'associatedcompanyid'
            ) = c.id

        WHERE d.resource = 'deals'
          AND c.resource = 'companies'
        """
    )

    client.close()
