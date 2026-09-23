import io
import json
import logging
from datetime import datetime, timezone

import pyarrow as pa
import pyarrow.parquet as pq
import requests

from config import Config

log = logging.getLogger(__name__)


class StorageService:
    """
    BE-2 storage layer.

    Flow:

        transformed deals
              ↓
           Parquet
              ↓
            MinIO
              ↓
         ClickHouse
    """

    def __init__(self):
        self.minio_endpoint = Config.MINIO_ENDPOINT.rstrip("/")
        self.minio_bucket = Config.MINIO_BUCKET

        self.minio_access_key = Config.MINIO_ACCESS_KEY
        self.minio_secret_key = Config.MINIO_SECRET_KEY

        self.clickhouse_host = Config.CLICKHOUSE_HOST
        self.clickhouse_port = Config.CLICKHOUSE_PORT
        self.clickhouse_user = Config.CLICKHOUSE_USER
        self.clickhouse_password = Config.CLICKHOUSE_PASSWORD
        self.clickhouse_database = Config.CLICKHOUSE_DATABASE

    # ---------------------------------------------------------
    # MinIO
    # ---------------------------------------------------------

    def _minio_request(
        self,
        method,
        object_path,
        data=None,
        content_type="application/octet-stream",
    ):
        """
        Temporary S3-compatible HTTP layer.

        We intentionally keep this dependency-light.
        Authentication/signing will be replaced with the
        production MinIO client layer once the pipeline works.
        """

        url = f"{self.minio_endpoint}/{self.minio_bucket}/{object_path}"

        response = requests.request(
            method,
            url,
            data=data,
            headers={
                "Content-Type": content_type,
            },
            timeout=60,
        )

        if not response.ok:
            raise RuntimeError(
                f"MinIO request failed: "
                f"{response.status_code} {response.text[:500]}"
            )

        return response

    def ensure_bucket(self):
        """
        Bucket creation will be handled by the MinIO bootstrap
        container in Docker Compose.

        This method is intentionally kept as a lifecycle hook.
        """
        return True

    def write_parquet(
        self,
        records,
        scan_id,
        tenant_id,
        page,
    ):
        """
        Convert transformed deals to Parquet and store them.

        Returns the object path.
        """

        records = list(records)

        if not records:
            return None

        table = pa.Table.from_pylist(records)

        buffer = io.BytesIO()

        pq.write_table(
            table,
            buffer,
            compression="snappy",
        )

        buffer.seek(0)

        timestamp = datetime.now(timezone.utc).strftime(
            "%Y%m%dT%H%M%SZ"
        )

        object_path = (
            f"tenants/{tenant_id}/"
            f"scans/{scan_id}/"
            f"deals/"
            f"page-{page:08d}-{timestamp}.parquet"
        )

        self._minio_request(
            "PUT",
            object_path,
            data=buffer.getvalue(),
            content_type="application/octet-stream",
        )

        log.info(
            "Stored %s records in MinIO: %s",
            len(records),
            object_path,
        )

        return object_path

    # ---------------------------------------------------------
    # ClickHouse
    # ---------------------------------------------------------

    def _clickhouse_url(self):
        return (
            f"http://{self.clickhouse_host}:"
            f"{self.clickhouse_port}"
        )

    def ensure_clickhouse_database(self):
        response = requests.post(
            self._clickhouse_url(),
            params={
                "query": (
                    f"CREATE DATABASE IF NOT EXISTS "
                    f"`{self.clickhouse_database}`"
                )
            },
            auth=(
                self.clickhouse_user,
                self.clickhouse_password,
            ),
            timeout=30,
        )

        if not response.ok:
            raise RuntimeError(
                f"ClickHouse database creation failed: "
                f"{response.status_code} {response.text[:500]}"
            )

    def ensure_clickhouse_table(self):
        self.ensure_clickhouse_database()

        query = f"""
        CREATE TABLE IF NOT EXISTS
        `{self.clickhouse_database}`.`deals`
        (
            id String,
            dealname Nullable(String),
            amount Nullable(Float64),
            dealstage Nullable(String),
            pipeline Nullable(String),
            closedate Nullable(String),
            createdate Nullable(String),
            lastmodifieddate Nullable(String),
            dealtype Nullable(String),
            description Nullable(String),
            archived Bool,
            _extracted_at String,
            _scan_id String,
            _tenant_id String
        )
        ENGINE = MergeTree
        ORDER BY ( _tenant_id, id )
        """

        response = requests.post(
            self._clickhouse_url(),
            params={"query": query},
            auth=(
                self.clickhouse_user,
                self.clickhouse_password,
            ),
            timeout=30,
        )

        if not response.ok:
            raise RuntimeError(
                f"ClickHouse table creation failed: "
                f"{response.status_code} {response.text[:500]}"
            )

    def write_clickhouse(self, records):
        records = list(records)

        if not records:
            return 0

        self.ensure_clickhouse_table()

        rows = []

        for record in records:
            rows.append(
                {
                    "id": str(record.get("id", "")),
                    "dealname": record.get("dealname"),
                    "amount": record.get("amount"),
                    "dealstage": record.get("dealstage"),
                    "pipeline": record.get("pipeline"),
                    "closedate": record.get("closedate"),
                    "createdate": record.get("createdate"),
                    "lastmodifieddate": record.get(
                        "lastmodifieddate"
                    ),
                    "dealtype": record.get("dealtype"),
                    "description": record.get("description"),
                    "archived": bool(
                        record.get("archived", False)
                    ),
                    "_extracted_at": record.get(
                        "_extracted_at"
                    ),
                    "_scan_id": record.get("_scan_id"),
                    "_tenant_id": record.get("_tenant_id"),
                }
            )

        payload = "\n".join(
            json.dumps(row, separators=(",", ":"))
            for row in rows
        )

        query = (
            f"INSERT INTO "
            f"`{self.clickhouse_database}`.`deals` "
            f"FORMAT JSONEachRow"
        )

        response = requests.post(
            self._clickhouse_url(),
            params={"query": query},
            data=payload.encode("utf-8"),
            auth=(
                self.clickhouse_user,
                self.clickhouse_password,
            ),
            headers={
                "Content-Type": "application/json",
            },
            timeout=60,
        )

        if not response.ok:
            raise RuntimeError(
                f"ClickHouse insert failed: "
                f"{response.status_code} {response.text[:500]}"
            )

        log.info(
            "Inserted %s records into ClickHouse",
            len(rows),
        )

        return len(rows)
