# Test Results

Automated unit coverage is included in `tests/`. Live HubSpot extraction evidence must be added after creating the required developer test account and five deals. No real access token is stored in this repository.

Required live validation:
- `curl http://localhost:5200/api/health`
- create a scan using a private-app token
- verify 5 deal IDs in PostgreSQL
- verify checkpoint/resume behavior
- open `/docs/`
