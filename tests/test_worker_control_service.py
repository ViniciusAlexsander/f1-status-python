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

    async def test_manual_live_start_stop_and_status(self) -> None:
        redis = FakeRedis()
        service = WorkerControlService(redis=redis)
        now = datetime(2026, 9, 21, 12, 0, tzinfo=timezone.utc)

        manual_live_until = await service.start_manual_live(30, now=now)
        await service.request_schedule_refresh()
        status = await service.get_status()

        self.assertEqual(manual_live_until, status.manualLiveUntil)
        self.assertTrue(status.scheduleRefreshRequested)

        await service.stop_manual_live()
        await service.clear_schedule_refresh_requested()
        status = await service.get_status()

        self.assertIsNone(status.manualLiveUntil)
        self.assertFalse(status.scheduleRefreshRequested)
