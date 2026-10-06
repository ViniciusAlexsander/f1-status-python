import json
from datetime import datetime, timezone
from typing import Any

from pydantic import BaseModel
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
    checkedAt: datetime


class WorkerControlService:
    SCHEDULE_KEY = "worker:schedule"

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

    async def get_status(self) -> WorkerStatus:
        return WorkerStatus(
            schedule=await self.get_schedule(),
            checkedAt=datetime.now(timezone.utc),
        )

    @staticmethod
    def _decode(value: Any) -> str:
        if isinstance(value, bytes):
            return value.decode("utf-8")

        if isinstance(value, str):
            return value

        return json.dumps(value)
