import os

os.environ.setdefault("DB_HOST", "localhost")

from unittest.mock import Mock
import pytest
from marshmallow import ValidationError

from services.data_source import transform_deal, create_deals_resource
from services.hubspot_api_service import HubSpotDealsAPIService, HubSpotAPIError
from api.schemas import StartScanSchema, PaginationSchema


def test_transform_deal():
    deal = {
        "id": "1",
        "properties": {
            "dealname": "Test Deal",
            "amount": "5000",
            "dealstage": "closedwon",
            "pipeline": "default",
            "hs_lastmodifieddate": "2026-09-18T10:00:00Z",
        },
        "archived": False,
    }

    result = transform_deal(deal, "scan-1", "tenant-1")

    assert result["id"] == "1"
    assert result["dealname"] == "Test Deal"
    assert result["amount"] == 5000.0
    assert result["dealstage"] == "closedwon"
    assert result["pipeline"] == "default"
    assert result["_scan_id"] == "scan-1"
    assert result["_tenant_id"] == "tenant-1"
    assert result["archived"] is False


def test_transform_invalid_amount():
    deal = {
        "id": "2",
        "properties": {
            "dealname": "Invalid Amount",
            "amount": "not-a-number",
        },
    }

    result = transform_deal(deal, "scan-2", "tenant-2")

    assert result["id"] == "2"
    assert result["amount"] is None


def test_transform_missing_properties():
    result = transform_deal(
        {"id": "3"},
        "scan-3",
        "tenant-3",
    )

    assert result["id"] == "3"
    assert result["dealname"] is None
    assert result["amount"] is None
    assert result["_scan_id"] == "scan-3"
    assert result["_tenant_id"] == "tenant-3"


def test_schema_defaults():
    data = StartScanSchema().load(
        {
            "scanId": "scan-1",
            "organizationId": "tenant-1",
            "auth": {"accessToken": "token"},
        }
    )

    assert data["type"] == "deals"
    assert data["filters"] == {}


def test_schema_requires_scan_id():
    with pytest.raises(ValidationError):
        StartScanSchema().load(
            {"organizationId": "tenant-1", "auth": {"accessToken": "token"}}
        )


def test_schema_requires_organization_id():
    with pytest.raises(ValidationError):
        StartScanSchema().load({"scanId": "scan-1", "auth": {"accessToken": "token"}})


def test_schema_requires_auth():
    with pytest.raises(ValidationError):
        StartScanSchema().load({"scanId": "scan-1", "organizationId": "tenant-1"})


def test_pagination_defaults():
    data = PaginationSchema().load({})

    assert data["limit"] == 20
    assert data["offset"] == 0


def test_pagination_rejects_invalid_limit():
    with pytest.raises(ValidationError):
        PaginationSchema().load({"limit": 0})


def test_pagination_rejects_negative_offset():
    with pytest.raises(ValidationError):
        PaginationSchema().load({"offset": -1})


def test_iter_deals_pagination():
    api = Mock(spec=HubSpotDealsAPIService)

    api.iter_deals.return_value = iter(
        [
            (
                1,
                {
                    "results": [
                        {
                            "id": "1",
                            "properties": {"dealname": "Deal 1", "amount": "1000"},
                        }
                    ],
                    "paging": {"next": {"after": "cursor-2"}},
                },
            ),
            (
                2,
                {
                    "results": [
                        {
                            "id": "2",
                            "properties": {"dealname": "Deal 2", "amount": "2000"},
                        }
                    ]
                },
            ),
        ]
    )

    resource = create_deals_resource(api, "token", "scan-1", "tenant-1")

    deals = list(resource())

    assert len(deals) == 2
    assert deals[0]["id"] == "1"
    assert deals[1]["id"] == "2"
    assert deals[0]["_tenant_id"] == "tenant-1"


def test_hubspot_missing_token():
    api = HubSpotDealsAPIService()

    with pytest.raises(HubSpotAPIError) as exc:
        api._request("", "https://api.hubapi.com/test")

    assert exc.value.status_code == 401


def test_hubspot_invalid_token():
    api = HubSpotDealsAPIService()

    response = Mock()
    response.status_code = 401
    response.ok = False
    response.text = "Unauthorized"

    api.session.get = Mock(return_value=response)

    with pytest.raises(HubSpotAPIError) as exc:
        api._request("invalid-token", "https://api.hubapi.com/test")

    assert exc.value.status_code == 401
    assert "Invalid or unauthorized" in str(exc.value)


def test_hubspot_forbidden():
    api = HubSpotDealsAPIService()

    response = Mock()
    response.status_code = 403
    response.ok = False
    response.text = "Forbidden"

    api.session.get = Mock(return_value=response)

    with pytest.raises(HubSpotAPIError) as exc:
        api._request("token", "https://api.hubapi.com/test")

    assert exc.value.status_code == 403
    assert "permissions" in str(exc.value)


def test_hubspot_not_found():
    api = HubSpotDealsAPIService()

    response = Mock()
    response.status_code = 404
    response.ok = False
    response.text = "Not Found"

    api.session.get = Mock(return_value=response)

    with pytest.raises(HubSpotAPIError) as exc:
        api._request("token", "https://api.hubapi.com/test")

    assert exc.value.status_code == 404


def test_hubspot_malformed_json():
    api = HubSpotDealsAPIService()

    response = Mock()
    response.status_code = 200
    response.ok = True
    response.json.side_effect = ValueError()

    api.session.get = Mock(return_value=response)

    with pytest.raises(HubSpotAPIError) as exc:
        api._request("token", "https://api.hubapi.com/test")

    assert "malformed JSON" in str(exc.value)


def test_hubspot_successful_request():
    api = HubSpotDealsAPIService()

    response = Mock()
    response.status_code = 200
    response.ok = True
    response.json.return_value = {"results": [{"id": "1"}]}

    api.session.get = Mock(return_value=response)

    result = api._request("valid-token", "https://api.hubapi.com/test")

    assert result["results"][0]["id"] == "1"


def test_hubspot_iter_deals():
    api = HubSpotDealsAPIService()

    api._request = Mock(
        return_value={"results": [{"id": "1", "properties": {"dealname": "Deal 1"}}]}
    )

    pages = list(api.iter_deals("valid-token", limit=5))

    assert len(pages) == 1
    assert pages[0][1]["results"][0]["id"] == "1"
    api._request.assert_called_once()
