import json
from datetime import datetime, timedelta, timezone
from typing import Any

from pydantic import BaseModel, Field
from redis.asyncio import Redis


class WorkerSchedule(BaseModel):
    raceId: str
    raceName: str
    sessionId: str
    sessionName: str
    startTime: datetime
    listenFrom: datetime
    listenUntil: datetime
    lastRefreshAt: datetime


class WorkerStatus(BaseModel):
    schedule: WorkerSchedule | None
    manualLiveUntil: datetime | None
    scheduleRefreshRequested: bool
    checkedAt: datetime


class StartManualLiveRequest(BaseModel):
    minutes: int = Field(default=240, ge=1, le=1440)


class WorkerControlService:
    SCHEDULE_KEY = "worker:schedule"
    MANUAL_LIVE_UNTIL_KEY = "worker:manual_live_until"
    SCHEDULE_REFRESH_REQUESTED_KEY = "worker:schedule_refresh_requested"

    def __init__(
        self,
        redis: Redis,
        schedule_ttl_seconds: int = 172800,
    ) -> None:
        self.redis = redis
        self.schedule_ttl_seconds = schedule_ttl_seconds

    async def get_schedule(self) -> WorkerSchedule | None:
        value = await self.redis.get(self.SCHEDULE_KEY)

        if value is None:
            return None

        return WorkerSchedule.model_validate_json(self._decode(value))

    async def save_schedule(self, schedule: WorkerSchedule) -> None:
        await self.redis.set(
            self.SCHEDULE_KEY,
            schedule.model_dump_json(),
            ex=self.schedule_ttl_seconds,
        )

    async def clear_schedule(self) -> None:
        await self.redis.delete(self.SCHEDULE_KEY)

    async def get_manual_live_until(self) -> datetime | None:
        value = await self.redis.get(self.MANUAL_LIVE_UNTIL_KEY)

        if value is None:
            return None

        return datetime.fromisoformat(self._decode(value))

    async def start_manual_live(
        self,
        minutes: int,
        now: datetime | None = None,
    ) -> datetime:
        resolved_now = now or datetime.now(timezone.utc)
        manual_live_until = resolved_now + timedelta(minutes=minutes)

        await self.redis.set(
            self.MANUAL_LIVE_UNTIL_KEY,
            manual_live_until.isoformat(),
            ex=minutes * 60,
        )

        return manual_live_until

    async def stop_manual_live(self) -> None:
        await self.redis.delete(self.MANUAL_LIVE_UNTIL_KEY)

    async def request_schedule_refresh(self) -> None:
        await self.redis.set(self.SCHEDULE_REFRESH_REQUESTED_KEY, "true")

    async def is_schedule_refresh_requested(self) -> bool:
        value = await self.redis.get(self.SCHEDULE_REFRESH_REQUESTED_KEY)
        return value is not None

    async def clear_schedule_refresh_requested(self) -> None:
        await self.redis.delete(self.SCHEDULE_REFRESH_REQUESTED_KEY)

    async def get_status(self) -> WorkerStatus:
        return WorkerStatus(
            schedule=await self.get_schedule(),
            manualLiveUntil=await self.get_manual_live_until(),
            scheduleRefreshRequested=await self.is_schedule_refresh_requested(),
            checkedAt=datetime.now(timezone.utc),
        )

    @staticmethod
    def _decode(value: Any) -> str:
        if isinstance(value, bytes):
            return value.decode("utf-8")

        if isinstance(value, str):
            return value

        return json.dumps(value)
