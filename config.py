import os
from dotenv import load_dotenv

load_dotenv()


class Config:
    APP_TITLE = "HubSpot BE-2 ingestion"
    APP_VERSION = "2.0.0"
    STATE_DIR = os.getenv("STATE_DIR", ".pipeline_state")
    COORDINATOR_HMAC_SECRET = os.getenv("COORDINATOR_HMAC_SECRET", "")
    TOKEN_ENCRYPTION_KEY = os.getenv("TOKEN_ENCRYPTION_KEY", "")
    MAX_CONCURRENT_SCANS = int(os.getenv("MAX_CONCURRENT_SCANS", "2"))
    RESOURCE_WORKERS = int(os.getenv("RESOURCE_WORKERS", "4"))
    HUBSPOT_API_BASE_URL = os.getenv(
        "HUBSPOT_API_BASE_URL", "https://api.hubapi.com"
    ).rstrip("/")
    HUBSPOT_API_TIMEOUT = int(os.getenv("HUBSPOT_API_TIMEOUT", "30"))
    HUBSPOT_RETRY_ATTEMPTS = int(os.getenv("HUBSPOT_RETRY_ATTEMPTS", "4"))
    HUBSPOT_RETRY_DELAY = float(os.getenv("HUBSPOT_RETRY_DELAY", "1"))
    MINIO_ENDPOINT = os.getenv("MINIO_ENDPOINT", "http://localhost:9000")
    MINIO_ACCESS_KEY = os.getenv("MINIO_ACCESS_KEY", "minioadmin")
    MINIO_SECRET_KEY = os.getenv("MINIO_SECRET_KEY", "minioadmin")
    MINIO_BUCKET = os.getenv("MINIO_BUCKET", "hubspot")
    MINIO_REGION = os.getenv("MINIO_REGION", "us-east-1")
    CLICKHOUSE_HOST = os.getenv("CLICKHOUSE_HOST", "localhost")
    CLICKHOUSE_PORT = int(os.getenv("CLICKHOUSE_PORT", "8123"))
    CLICKHOUSE_USER = os.getenv("CLICKHOUSE_USER", "default")
    CLICKHOUSE_PASSWORD = os.getenv("CLICKHOUSE_PASSWORD", "")
    CLICKHOUSE_DATABASE = os.getenv("CLICKHOUSE_DATABASE", "analytics")
