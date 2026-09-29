"""dlt filesystem (S3/MinIO) -> Parquet -> ClickHouse atomic batch replacement."""

import hashlib
import re
import shutil
from pathlib import Path
from threading import Lock
from urllib.parse import urlparse

import clickhouse_connect
import dlt
import pyarrow as pa
import pyarrow.parquet as pq
from dlt.destinations import filesystem
from minio import Minio

from config import Config

COLUMNS = [
    "id",
    "resource",
    "properties",
    "payload",
    "company_ids",
    "archived",
    "scan_id",
    "tenant_id",
    "updated_at",
    "extracted_at",
    "batch_id",
]
TABLE_SCHEMA = """(
    id String, resource LowCardinality(String), properties String, payload String,
    company_ids String, archived Bool, scan_id String, tenant_id String,
    updated_at String, extracted_at String, batch_id String
) ENGINE = MergeTree PARTITION BY batch_id ORDER BY (tenant_id, resource, id)"""


class StorageService:
    def __init__(self, state_dir):
        self.root = Path(state_dir) / "parquet"
        self.root.mkdir(parents=True, exist_ok=True)
        endpoint = urlparse(Config.MINIO_ENDPOINT)
        self.minio = Minio(
            endpoint.netloc,
            access_key=Config.MINIO_ACCESS_KEY,
            secret_key=Config.MINIO_SECRET_KEY,
            secure=endpoint.scheme == "https",
            region=Config.MINIO_REGION,
        )
        if not re.fullmatch(r"[A-Za-z_][A-Za-z0-9_]*", Config.CLICKHOUSE_DATABASE):
            raise ValueError("Invalid CLICKHOUSE_DATABASE")
        self.database = Config.CLICKHOUSE_DATABASE
        self.lock = Lock()
        self.ready = False

    def client(self, database=None):
        return clickhouse_connect.get_client(
            host=Config.CLICKHOUSE_HOST,
            port=Config.CLICKHOUSE_PORT,
            username=Config.CLICKHOUSE_USER,
            password=Config.CLICKHOUSE_PASSWORD,
            database=database or self.database,
        )

    def initialize(self):
        with self.lock:
            if self.ready:
                return
            if not self.minio.bucket_exists(Config.MINIO_BUCKET):
                self.minio.make_bucket(Config.MINIO_BUCKET)
            client = self.client("default")
            try:
                client.command(f"CREATE DATABASE IF NOT EXISTS `{self.database}`")
            finally:
                client.close()
            client = self.client()
            try:
                # New table names deliberately avoid silently reusing legacy schemas.
                client.command(
                    f"CREATE TABLE IF NOT EXISTS hubspot_batches {TABLE_SCHEMA}"
                )
                client.command("""CREATE OR REPLACE VIEW hubspot_records AS
                    SELECT tenant_id, resource, id,
                        tupleElement(latest, 1) AS properties,
                        tupleElement(latest, 2) AS payload,
                        tupleElement(latest, 3) AS company_ids,
                        tupleElement(latest, 4) AS archived,
                        tupleElement(latest, 5) AS scan_id,
                        tupleElement(latest, 6) AS updated_at
                    FROM (
                        SELECT tenant_id, resource, id,
                            argMax(tuple(properties, payload, company_ids, archived, scan_id, updated_at),
                                   tuple(updated_at, extracted_at, batch_id)) AS latest
                        FROM hubspot_batches GROUP BY tenant_id, resource, id
                    )""")
                client.command("""CREATE OR REPLACE VIEW v_hubspot_deal_pipeline AS
                    SELECT d.tenant_id, d.id AS deal_id,
                        JSONExtractString(d.properties, 'dealname') AS deal_name,
                        toFloat64OrNull(JSONExtractString(d.properties, 'amount')) AS amount,
                        JSONExtractString(d.properties, 'dealstage') AS deal_stage,
                        JSONExtractString(d.properties, 'pipeline') AS pipeline_id,
                        JSONExtractString(p.properties, 'label') AS pipeline_name,
                        company_id, JSONExtractString(c.properties, 'name') AS company_name,
                        JSONExtractString(c.properties, 'domain') AS company_domain
                    FROM (
                        SELECT *, arrayJoin(if(empty(JSONExtract(company_ids, 'Array(String)')),
                            [''], JSONExtract(company_ids, 'Array(String)'))) AS company_id
                        FROM hubspot_records WHERE resource='deals' AND NOT archived
                    ) d
                    LEFT JOIN (SELECT * FROM hubspot_records WHERE resource='companies' AND NOT archived) c
                        ON d.tenant_id=c.tenant_id AND d.company_id=c.id
                    LEFT JOIN (SELECT * FROM hubspot_records WHERE resource='pipelines' AND NOT archived) p
                        ON d.tenant_id=p.tenant_id AND JSONExtractString(d.properties, 'pipeline')=p.id
                """)
            finally:
                client.close()
            self.ready = True

    def load(self, pending):
        if not pending["records"]:
            return
        self.initialize()
        batch = pending["batch_id"]
        if not re.fullmatch("[a-f0-9]{64}", batch):
            raise ValueError("Invalid batch ID")
        work = self.root / batch
        # The journal owns recovery, so any partial dlt package is safely rebuilt.
        shutil.rmtree(work, ignore_errors=True)
        rows = [dict(record, batch_id=batch) for record in pending["records"]]
        date = pending["extracted_at"]
        tenant = hashlib.sha256(rows[0]["tenant_id"].encode()).hexdigest()
        suffix = f"year={date[:4]}/month={date[5:7]}/tenant={tenant}/{batch}"
        prefix = f"hubspot/{pending['resource']}/{suffix}/"
        # One owner per batch; the durable payload can recreate any partial upload.
        for obj in self.minio.list_objects(
            Config.MINIO_BUCKET, prefix=prefix, recursive=True
        ):
            self.minio.remove_object(Config.MINIO_BUCKET, obj.object_name)
        destination = filesystem(
            bucket_url=f"s3://{Config.MINIO_BUCKET}",
            credentials={
                "aws_access_key_id": Config.MINIO_ACCESS_KEY,
                "aws_secret_access_key": Config.MINIO_SECRET_KEY,
                "endpoint_url": Config.MINIO_ENDPOINT,
                "region_name": Config.MINIO_REGION,
            },
            layout="{table_name}/" + suffix + "/{file_id}.{ext}",
        )
        pipeline = dlt.pipeline(
            pipeline_name=f"batch_{batch}",
            pipelines_dir=str(work / "dlt"),
            destination=destination,
            dataset_name="hubspot",
            restore_from_destination=False,
        )
        try:
            resource = dlt.resource(
                rows, name=pending["resource"], write_disposition="append"
            )
            pipeline.run(resource, loader_file_format="parquet")
            files = [
                obj.object_name
                for obj in self.minio.list_objects(
                    Config.MINIO_BUCKET, prefix=prefix, recursive=True
                )
                if obj.object_name.endswith(".parquet")
            ]
            if not files:
                raise RuntimeError("dlt produced no resource Parquet files")
            tables = []
            for key in sorted(files):
                # Load the landed object, not the in-memory API response.
                response = self.minio.get_object(Config.MINIO_BUCKET, key)
                try:
                    tables.append(
                        pq.read_table(pa.BufferReader(response.read())).select(COLUMNS)
                    )
                finally:
                    response.close()
                    response.release_conn()
            table = pa.concat_tables(tables)
            # One staging table per durable page, owned by one resource worker.
            # TRUNCATE + INSERT can be retried; only REPLACE exposes the full page.
            stage = f"stage_{batch}"
            client = self.client()
            try:
                client.command(f"CREATE TABLE IF NOT EXISTS {stage} {TABLE_SCHEMA}")
                client.command(f"TRUNCATE TABLE {stage}")
                client.insert_arrow(stage, table)
                client.command(
                    f"ALTER TABLE hubspot_batches REPLACE PARTITION '{batch}' FROM {stage}"
                )
                client.command(f"DROP TABLE {stage}")
            finally:
                client.close()
        finally:
            pipeline.deactivate()
            shutil.rmtree(work, ignore_errors=True)
