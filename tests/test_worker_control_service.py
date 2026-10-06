import unittest
from datetime import datetime, timezone

from api.services.worker_control_service import WorkerControlService, WorkerSchedule


class FakeRedis:
    def __init__(self) -> None:
        self.values = {}
        self.expirations = {}

    async def get(self, key: str):
        return self.values.get(key)

    async def set(self, key: str, value: str, ex: int | None = None) -> None:
        self.values[key] = value
        self.expirations[key] = ex

    async def delete(self, key: str) -> None:
        self.values.pop(key, None)
        self.expirations.pop(key, None)


class WorkerControlServiceTest(unittest.IsolatedAsyncioTestCase):
    async def test_saves_and_reads_worker_schedule(self) -> None:
        redis = FakeRedis()
        service = WorkerControlService(redis=redis, schedule_ttl_seconds=123)
        now = datetime(2026, 9, 21, 12, 0, tzinfo=timezone.utc)
        schedule = WorkerSchedule(
            raceId="race-1",
            raceName="Race 1",
            sessionId="session-1",
            sessionName="Practice",
            startTime=now,
            listenFrom=now,
            listenUntil=now,
            lastRefreshAt=now,
        )

        await service.save_schedule(schedule)
        restored = await service.get_schedule()

        self.assertEqual(restored, schedule)
        self.assertEqual(redis.expirations[service.SCHEDULE_KEY], 123)

    async def test_status_returns_schedule_without_manual_controls(self) -> None:
        redis = FakeRedis()
        service = WorkerControlService(redis=redis)
        status = await service.get_status()

        self.assertIsNone(status.schedule)
        self.assertIsNotNone(status.checkedAt.tzinfo)
        self.assertNotIn("scheduleRefreshRequested", type(status).model_fields)
