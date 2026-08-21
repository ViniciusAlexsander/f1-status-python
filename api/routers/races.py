from fastapi import APIRouter, Depends, HTTPException, status

from api.dependencies import get_formula1_race_service
from api.domain.ocblacktop_client import (
    OcblacktopClientError,
    OcblacktopClientInvalidResponseError,
    OcblacktopClientTimeoutError,
)
from api.schemas.formula1_events import RaceListResponse
from api.schemas.session_results import SessionResultsResponse
from api.services.race_service import RaceService

router = APIRouter(prefix="/races", tags=["Formula 1"])


@router.get("", response_model=RaceListResponse)
async def list_race(
    service: RaceService = Depends(get_formula1_race_service),
) -> RaceListResponse:
    try:
        return await service.list_races()

    except OcblacktopClientTimeoutError as exc:
        raise HTTPException(
            status_code=status.HTTP_504_GATEWAY_TIMEOUT,
            detail="External API timeout",
        ) from exc

    except OcblacktopClientInvalidResponseError as exc:
        raise HTTPException(
            status_code=status.HTTP_502_BAD_GATEWAY,
            detail="External API returned an invalid response",
        ) from exc

    except OcblacktopClientError as exc:
        raise HTTPException(
            status_code=status.HTTP_502_BAD_GATEWAY,
            detail=(
                f"External API request failed with status {exc.status_code}"
                if exc.status_code
                else "External API request failed"
            ),
        ) from exc

@router.get("/{event_id}/sessions/{session_id}/results", response_model=list[SessionResultsResponse])
async def get_race_results(
    event_id: str,
    session_id: str,
    service: RaceService = Depends(get_formula1_race_service),
) -> list[SessionResultsResponse]:
    try:
        return await service.get_race_results(event_id, session_id)

    except OcblacktopClientTimeoutError as exc:
        raise HTTPException(
            status_code=status.HTTP_504_GATEWAY_TIMEOUT,
            detail="External API timeout",
        ) from exc

    except OcblacktopClientInvalidResponseError as exc:
        raise HTTPException(
            status_code=status.HTTP_502_BAD_GATEWAY,
            detail="External API returned an invalid response",
        ) from exc

    except OcblacktopClientError as exc:
        raise HTTPException(
            status_code=status.HTTP_502_BAD_GATEWAY,
            detail=(
                f"External API request failed with status {exc.status_code}"
                if exc.status_code
                else "External API request failed"
            ),
        ) from exc
