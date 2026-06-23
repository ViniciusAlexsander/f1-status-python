from fastapi import Depends

from core.config import Settings, get_settings
from domain.ocblacktop_client import OcblacktopClient
from services.formula1_events_service import Formula1EventsService

def get_ocblacktop_client(
    settings: Settings = Depends(get_settings),
) -> OcblacktopClient:
    return OcblacktopClient(
        base_url=settings.ocblacktop_api_base_url,
        api_key=settings.ocblacktop_api_key,
    )


def get_formula1_events_service(
    client: OcblacktopClient = Depends(get_ocblacktop_client),
) -> Formula1EventsService:
    return Formula1EventsService(client=client)