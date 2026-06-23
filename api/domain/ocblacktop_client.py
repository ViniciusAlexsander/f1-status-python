import httpx
from pydantic import ValidationError

from schemas.formula1_events import Formula1EventsResponse

class OcblacktopClientError(Exception):
    pass

class OcblacktopClientTimeoutError(Exception):
    pass

class OcblacktopClientInvalidResponseError(Exception):
    pass

class OcblacktopClient:
    def __init__(self, base_url: str, api_key: str, timeout: float = 10.0):
        self.base_url = base_url.rstrip("/")
        self.api_key = api_key
        self.timeout = timeout

    async def get_f1_events(
        self,
        year: int,
        limit: int,
    ) -> Formula1EventsResponse:
        url = f"{self.base_url}/formula1/events"

        headers = {
            "Accept": "application/json",
            "X-API-Key": self.api_key,
        }

        params = {
            "year": year,
            "limit": limit,
        }

        try:
            async with httpx.AsyncClient(timeout=self.timeout) as client:
                response = await client.get(url, headers=headers, params=params)
                response.raise_for_status()
        except httpx.TimeoutException as exc:
            raise OcblacktopClientTimeoutError("OC Blacktop request timed out") from exc

        except httpx.HTTPStatusError as exc:
            raise OcblacktopClientError(
                f"OC Blacktop returned status {exc.response.status_code}"
            ) from exc

        except httpx.HTTPError as exc:
            raise OcblacktopClientError("OC Blacktop request failed") from exc

        try:
            return Formula1EventsResponse.model_validate(response.json())

        except ValidationError as exc:
            raise OcblacktopClientInvalidResponseError(
                "OC Blacktop returned an invalid response"
            ) from exc
