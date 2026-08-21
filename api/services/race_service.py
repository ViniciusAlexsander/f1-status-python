from api.domain.ocblacktop_client import OcblacktopClient
from api.schemas.formula1_events import RaceListData, RaceListResponse
from datetime import date

from api.schemas.session_results import SessionResultsResponse

LIMIT = 25

class RaceService:
    def __init__(self, client: OcblacktopClient):
        self.client = client

    async def list_races(self) -> RaceListResponse:
        today = date.today()
        current_year = today.year
        response = await self.client.get_f1_events(year=current_year, limit=LIMIT)

        sorted_races = sorted(response.data, key=lambda event: event.dateStart)

        next_race = next((race for race in sorted_races if race.status == "scheduled" and race.dateStart >= today), None)

        current_race = next((race for race in sorted_races if race.status == "ongoing"), None)

        data = RaceListData(
            races=sorted_races,
            nextRace=next_race,
            currentRace=current_race
        )

        return RaceListResponse(
            data=data,
            meta=response.meta,
        )

    async def get_race_results(self, event_id: str, session_id: str) -> list[SessionResultsResponse]:
        response = await self.client.get_sessions_results(eventId=event_id, sessionId=session_id)
        return response