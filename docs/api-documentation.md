# Service API Documentation

Base URL: `http://localhost:5200/api`

## Start scan
`POST /v1/scan/start`

```json
{"scanId":"scan-001","organizationId":"123456","type":"deals","auth":{"accessToken":"REDACTED"},"filters":{}}
```
Returns HTTP 202 with `scanId` and status.

## Status
`GET /v1/scan/{scan_id}/status`

## Cancel
`POST /v1/scan/{scan_id}/cancel`

## List scans
`GET /v1/scan/list?organizationId=123456&limit=20&offset=0`

## Statistics
`GET /v1/scan/statistics`

## Result
`GET /v1/scan/{scan_id}/result`

## Remove
`DELETE /v1/scan/{scan_id}/remove`

## Health
`GET /api/health`

Errors use JSON with `success:false` and `message`; common statuses are 400, 404, 409, 502, and 503.
