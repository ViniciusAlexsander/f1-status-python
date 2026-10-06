from fastapi import APIRouter, Depends, Header, HTTPException, status

from api.core.config import Settings, get_settings
from api.dependencies import get_worker_control_service
from api.services.worker_control_service import (
    WorkerControlService,
    WorkerStatus,
)

router = APIRouter(prefix="/worker", tags=["Worker"])


def require_worker_admin_token(
    x_worker_admin_token: str | None = Header(default=None),
    settings: Settings = Depends(get_settings),
) -> None:
    if not settings.worker_admin_token:
        raise HTTPException(
            status_code=status.HTTP_503_SERVICE_UNAVAILABLE,
            detail="Worker admin token is not configured",
        )

    if x_worker_admin_token != settings.worker_admin_token:
        raise HTTPException(
            status_code=status.HTTP_403_FORBIDDEN,
            detail="Invalid worker admin token",
        )


@router.get(
    "/status",
    response_model=WorkerStatus,
    dependencies=[Depends(require_worker_admin_token)],
)
async def get_worker_status(
    service: WorkerControlService = Depends(get_worker_control_service),
) -> WorkerStatus:
    return await service.get_status()
