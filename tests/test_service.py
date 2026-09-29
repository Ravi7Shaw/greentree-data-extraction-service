from unittest.mock import Mock
import pytest
from services.hubspot_api_service import HubSpotDealsAPIService, HubSpotAPIError


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


@pytest.mark.parametrize(
    "headers, expected",
    [
        ({"Retry-After": "2", "X-HubSpot-RateLimit-Interval-Milliseconds": "500"}, 2),
        ({"X-HubSpot-RateLimit-Interval-Milliseconds": "1500"}, 1.5),
        ({"Retry-After": "invalid"}, 1),
    ],
)
def test_retry_header_precedence(monkeypatch, headers, expected):
    sleep = Mock()
    monkeypatch.setattr("services.hubspot_api_service.time.sleep", sleep)
    api = HubSpotDealsAPIService()
    retry = Mock(status_code=429, headers=headers)
    success = Mock(status_code=200, ok=True)
    success.json.return_value = {"results": []}
    api.session.get = Mock(side_effect=[retry, success])
    api._request("token", "https://api.hubapi.com/test")
    sleep.assert_called_once_with(expected)
