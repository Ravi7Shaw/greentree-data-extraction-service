import logging, time
from typing import Any, Dict, Iterator, List, Optional
import requests

log = logging.getLogger(__name__)


class HubSpotAPIError(RuntimeError):
    def __init__(self, message, status_code=None, retryable=False):
        self.status_code = status_code
        self.retryable = retryable
        super().__init__(message)


class HubSpotDealsAPIService:
    endpoint = "/crm/v3/objects/deals"
    properties_endpoint = "/crm/v3/properties/deals"

    def __init__(
        self, base_url="https://api.hubapi.com", timeout=30, retries=4, retry_delay=1.0
    ):
        self.base_url = base_url.rstrip("/")
        self.timeout = timeout
        self.retries = retries
        self.retry_delay = retry_delay
        self.session = requests.Session()
        self.session.headers.update(
            {
                "Accept": "application/json",
                "Content-Type": "application/json",
                "User-Agent": "hubspot-deals-etl/1.0",
            }
        )

    def _request(self, token, url, params=None):
        if not token:
            raise HubSpotAPIError("Missing HubSpot access token", 401)
        for attempt in range(self.retries + 1):
            try:
                r = self.session.get(
                    url,
                    headers={"Authorization": f"Bearer {token}"},
                    params=params,
                    timeout=self.timeout,
                )
            except requests.RequestException as e:
                if attempt >= self.retries:
                    raise HubSpotAPIError(
                        f"HubSpot request failed: {e}", retryable=True
                    ) from e
                time.sleep(self.retry_delay * (2**attempt))
                continue
            if r.status_code == 429 or 500 <= r.status_code < 600:
                if attempt >= self.retries:
                    raise HubSpotAPIError(
                        f"HubSpot API error {r.status_code}: {r.text[:500]}",
                        r.status_code,
                        True,
                    )
                retry_after = r.headers.get("Retry-After") or r.headers.get(
                    "X-HubSpot-RateLimit-Interval-Milliseconds"
                )
                delay = (
                    float(retry_after) / 1000
                    if retry_after
                    and str(retry_after).isdigit()
                    and "Milliseconds"
                    in (
                        "X-HubSpot-RateLimit-Interval-Milliseconds"
                        if "X-HubSpot-RateLimit-Interval-Milliseconds" in r.headers
                        else ""
                    )
                    else float(retry_after)
                    if retry_after
                    else self.retry_delay * (2**attempt)
                )
                time.sleep(min(delay, 60))
                continue
            if r.status_code == 401:
                raise HubSpotAPIError(
                    "Invalid or unauthorized HubSpot access token", 401
                )
            if r.status_code == 403:
                raise HubSpotAPIError("HubSpot token lacks required permissions", 403)
            if r.status_code == 404:
                raise HubSpotAPIError("HubSpot endpoint not found", 404)
            if not r.ok:
                raise HubSpotAPIError(
                    f"HubSpot API error {r.status_code}: {r.text[:500]}", r.status_code
                )
            try:
                return r.json()
            except ValueError as e:
                raise HubSpotAPIError(
                    "HubSpot returned malformed JSON", r.status_code
                ) from e
        raise HubSpotAPIError("HubSpot request exhausted retries", retryable=True)

    def validate_credentials(self, token):
        try:
            self._request(token, self.base_url + self.endpoint, {"limit": 1})
            return True
        except HubSpotAPIError as e:
            log.error("HubSpot credential validation failed: %s", e)
            return False

    def get_deal_properties(self, token):
        return self._request(token, self.properties_endpoint)

    def iter_deals(self, token, limit=100, properties=None, archived=False, after=None):
        props = properties or [
            "dealname",
            "amount",
            "dealstage",
            "pipeline",
            "closedate",
            "createdate",
            "hs_lastmodifieddate",
            "dealtype",
            "description",
        ]
        cursor = after
        page = 0
        while True:
            params = {
                "limit": min(max(int(limit), 1), 100),
                "properties": ",".join(props),
                "archived": str(bool(archived)).lower(),
            }
            if cursor:
                params["after"] = cursor
            data = self._request(token, self.base_url + self.endpoint, params)
            page += 1
            yield page, data
            cursor = ((data.get("paging") or {}).get("next") or {}).get("after")
            if not cursor:
                break
