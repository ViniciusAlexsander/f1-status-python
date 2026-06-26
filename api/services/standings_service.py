from datetime import date

from api.domain.ocblacktop_client import OcblacktopClient
from api.schemas.constructors_standings import TeamStandingData
from api.schemas.drivers_standings import DriverStandingData


class StandingsService:
    def __init__(self, client: OcblacktopClient):
        self.client = client
    
    async def drivers_standings(self) -> list[DriverStandingData]:
        today = date.today()
        current_year = today.year
        response = await self.client.get_drivers_standings(current_year)

        return response
    
    async def constructors_standings(self) -> list[TeamStandingData]:
        today = date.today()
        current_year = today.year
        response = await self.client.get_constructors_standings(current_year)

        return response