from fastapi import APIRouter, Depends, HTTPException, Query, status

from dependencies import get_formula1_events_service
from domain.ocblacktop_client import (
    OcblacktopClientError,
    OcblacktopClientInvalidResponseError,
    OcblacktopClientTimeoutError,
)
from schemas.formula1_events import Formula1EventsResponse
from services.formula1_events_service import Formula1EventsService

router = APIRouter(prefix="/api/v1/formula1", tags=["Formula 1"])


@router.get("/events", response_model=Formula1EventsResponse)
async def list_formula1_events(
    service: Formula1EventsService = Depends(get_formula1_events_service),
) -> Formula1EventsResponse:
    try:
        return await service.list_events()

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
            detail="External API request failed",
        ) from exc