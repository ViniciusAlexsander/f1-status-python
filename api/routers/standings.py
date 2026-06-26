from api.schemas.constructors_standings import TeamStandingData
from api.schemas.drivers_standings import DriverStandingData
from api.services.standings_service import StandingsService
from fastapi import APIRouter, Depends, HTTPException, status

from api.dependencies import get_standings_service
from api.domain.ocblacktop_client import (
    OcblacktopClientError,
    OcblacktopClientInvalidResponseError,
    OcblacktopClientTimeoutError,
)

router = APIRouter(prefix="/standings", tags=["Formula 1"])

@router.get("/drivers", response_model= list[DriverStandingData])
async def drivers_standings(
    service: StandingsService = Depends(get_standings_service)
) ->  list[DriverStandingData]:
    try:
        return await service.drivers_standings()
    
    except Exception as exc:
        raise HTTPException(
            status_code=status.HTTP_502_BAD_GATEWAY,
            detail=(
                f"External API request failed with status {exc.status_code}"
                if exc.status_code
                else "External API request failed"
            ),
        ) from exc
    
@router.get("/constructors", response_model= list[TeamStandingData])
async def constructors_standings(
    service: StandingsService = Depends(get_standings_service)
) ->  list[TeamStandingData]:
    try:
        return await service.constructors_standings()
    
    except Exception as exc:
        raise HTTPException(
            status_code=status.HTTP_502_BAD_GATEWAY,
            detail=(
                f"External API request failed with status {exc.status_code}"
                if exc.status_code
                else "External API request failed"
            ),
        ) from exc