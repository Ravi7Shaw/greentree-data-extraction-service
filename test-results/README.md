# BE-2 validation evidence

Run from the repository with Python 3.12.11 and installed requirements:

```bash
.venv/bin/python -m pytest -q -m 'not integration'
BE2_INTEGRATION=1 .venv/bin/python -m pytest -q -m integration
```

The integration suite requires local MinIO and ClickHouse configured through `.env`. It creates unique disposable buckets/databases and removes them after each test. The HubSpot HTTP fixture contains synthetic records only.

Assertions include all eight resource adapters; Coordinator signature tampering, nonce replay and tenant isolation; encrypted credentials; inclusive watermarks; checkpoint-after-load ordering; parallel worker overlap; pause/resume across supervisor restart; compressed Parquet in MinIO; deal/company/pipeline view results; and actual `SIGKILL` recovery. The crash test requires 16 physical ClickHouse rows and 16 current records after replay. The reporting query also retains a deal without a company.

Real HubSpot account permissions and an external Coordinator have not been exercised. Do not present synthetic fixtures as evidence of live account extraction.
