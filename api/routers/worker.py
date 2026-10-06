from fastapi import APIRouter, Depends, Header, HTTPException, status

from api.core.config import Settings, get_settings
from api.dependencies import get_worker_control_service
from api.services.worker_control_service import (
    StartManualLiveRequest,
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


@router.post("/live/start", dependencies=[Depends(require_worker_admin_token)])
async def start_manual_live(
    request: StartManualLiveRequest,
    service: WorkerControlService = Depends(get_worker_control_service),
) -> dict[str, str]:
    manual_live_until = await service.start_manual_live(request.minutes)
    return {"manualLiveUntil": manual_live_until.isoformat()}


@router.post("/live/stop", dependencies=[Depends(require_worker_admin_token)])
async def stop_manual_live(
    service: WorkerControlService = Depends(get_worker_control_service),
) -> dict[str, str]:
    await service.stop_manual_live()
    return {"status": "stopped"}


@router.post("/schedule/refresh", dependencies=[Depends(require_worker_admin_token)])
async def request_schedule_refresh(
    service: WorkerControlService = Depends(get_worker_control_service),
) -> dict[str, str]:
    await service.request_schedule_refresh()
    return {"status": "requested"}


@router.get(
    "/status",
    response_model=WorkerStatus,
    dependencies=[Depends(require_worker_admin_token)],
)
async def get_worker_status(
    service: WorkerControlService = Depends(get_worker_control_service),
) -> WorkerStatus:
    return await service.get_status()
