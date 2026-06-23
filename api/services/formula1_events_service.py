from domain.ocblacktop_client import OcblacktopClient
from schemas.formula1_events import Formula1EventsResponse
import datetime

LIMIT = 25

class Formula1EventsService:
    def __init__(self, client: OcblacktopClient):
        self.client = client

    async def list_events(self) -> Formula1EventsResponse:
        current_year = datetime.date.today().year
        response = await self.client.get_f1_events(year=current_year, limit=LIMIT)

        sorted_events = sorted(response.data, key=lambda event: event.dateStart)

        return Formula1EventsResponse(
            data=sorted_events,
            meta=response.meta,
        )