# BE-2 storage schema

## SQLite journal (`STATE_DIR/state.sqlite3`)

`scans` stores unique ID, tenant, encrypted HubSpot credential, creation timestamp and JSON lifecycle/checkpoints. Removed scans leave an ID/tenant tombstone and erase the encrypted credential. `watermarks` stores completed updated-at cursors per tenant/resource. `nonces` stores consumed Coordinator nonces until expiration. WAL and `synchronous=FULL` protect atomic commits across process crashes. Back up the directory consistently and retain `TOKEN_ENCRYPTION_KEY` separately.

Each resource checkpoint records `cursor` (next page), `page`, `records`, `done`, `watermark` (run's lower bound), `updated_at` (maximum observed), and `pending` (durable payload/batch metadata). A pending payload is cleared only after both storage systems acknowledge the load.

## MinIO

Bucket: `MINIO_BUCKET`. dlt filesystem destination uses S3 credentials and the configured endpoint. Files are compressed Parquet with stable columns: `id`, `resource`, `properties`, `payload`, `company_ids`, `archived`, `scan_id`, `tenant_id`, `updated_at`, `extracted_at`, `batch_id`, plus dlt metadata.

`properties` and `payload` are JSON strings: new/custom fields survive schema changes and nested owner/pipeline/engagement data is retained. `company_ids` preserves deal/company associations. Dataset path: `hubspot/<resource>/year=YYYY/month=MM/tenant=<sha256>/<batch-id>/`. dlt also writes its internal metadata under the dataset.

## ClickHouse

`hubspot_batches`: MergeTree, partitioned by deterministic page batch ID, ordered by `(tenant_id, resource, id)`. Each page loads through a `stage_<batch-id>` table then `REPLACE PARTITION`. A crash at any point can retry the page; readers see complete partition replacements. Source history across different scans intentionally remains in this table.

`hubspot_records`: latest-value view grouped by `(tenant_id, resource, id)` using updated-at timestamp, extraction timestamp and batch ID as a deterministic version tuple. It exposes one record per source identity even across repeated snapshots.

`v_hubspot_deal_pipeline`: analytical LEFT JOIN of current non-archived deals, associated companies and deal pipelines with tenant predicates in both joins. One row per deal/company association; a deal with no company retains a row. Multiple associated companies intentionally produce multiple reporting rows, so aggregate deal amounts at deal grain.

The API's old PostgreSQL tables are no longer used. Existing legacy ClickHouse `deals`/`hubspot_records` tables are not migrated automatically: use a new `CLICKHOUSE_DATABASE` or migrate them before startup. The new implementation expects `hubspot_records` to be a view. No historical data is deleted by this change.

Retention and partition compaction must be designed for production volume. Dropping scan metadata does not delete output or watermarks.
