from api.services.standings_service import StandingsService
from fastapi import Depends

from api.core.config import Settings, get_settings
from api.domain.ocblacktop_client import OcblacktopClient
from api.services.race_service import RaceService

def get_ocblacktop_client(
    settings: Settings = Depends(get_settings),
) -> OcblacktopClient:
    return OcblacktopClient(
        base_url=settings.ocblacktop_api_base_url,
        api_key=settings.ocblacktop_api_key,
    )


def get_formula1_events_service(
    client: OcblacktopClient = Depends(get_ocblacktop_client),
) -> RaceService:
    return RaceService(client=client)

def get_standings_service(
    client: OcblacktopClient = Depends(get_ocblacktop_client),
) -> StandingsService:
    return StandingsService(client=client)