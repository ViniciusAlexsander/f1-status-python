import asyncio
from datetime import date

from api.domain.ocblacktop_client import OcblacktopClient
from api.schemas.formula1_events import Race, RaceListData, RaceListResponse
from api.schemas.session_results import SessionResultsResponse
from api.services.weather_forecast_service import WeatherForecastService

LIMIT = 25

class RaceService:
    def __init__(
        self,
        client: OcblacktopClient,
        weather_forecast_service: WeatherForecastService | None = None,
    ):
        self.client = client
        self.weather_forecast_service = weather_forecast_service or WeatherForecastService(
            client
        )

    async def _maybe_enrich_race(self, race: Race | None) -> Race | None:
        if race is None:
            return None
        return await self.weather_forecast_service.enrich_race(race)

    async def list_races(self) -> RaceListResponse:
        today = date.today()
        current_year = today.year
        response = await self.client.get_f1_events(year=current_year, limit=LIMIT)

        sorted_races = sorted(response.data, key=lambda event: event.dateStart)

        next_race = next((race for race in sorted_races if race.status == "scheduled" and race.dateStart >= today), None)

        current_race = next((race for race in sorted_races if race.status == "ongoing"), None)

        enriched_current, enriched_next = await asyncio.gather(
            self._maybe_enrich_race(current_race),
            self._maybe_enrich_race(next_race),
        )

        data = RaceListData(
            races=sorted_races,
            nextRace=enriched_next,
            currentRace=enriched_current,
        )

        return RaceListResponse(
            data=data,
            meta=response.meta,
        )

    async def get_race_results(self, event_id: str, session_id: str) -> list[SessionResultsResponse]:
        response = await self.client.get_sessions_results(eventId=event_id, sessionId=session_id)
        return response