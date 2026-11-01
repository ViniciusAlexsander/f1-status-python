import fnmatch
import json
import unittest
from datetime import date

from api.services.live_timing_cache import LiveTimingCache


class FakeRedis:
    def __init__(self) -> None:
        self.values: dict[str, str] = {}

    async def get(self, key: str) -> str | None:
        return self.values.get(key)

    async def set(self, key: str, value: str) -> None:
        self.values[key] = value

    async def publish(self, channel: str, value: str) -> None:
        pass

    async def scan_iter(self, match: str):
        for key in self.values:
            if fnmatch.fnmatch(key, match):
                yield key


class LiveTimingCacheTest(unittest.IsolatedAsyncioTestCase):
    async def test_get_snapshot_migrates_latest_session_cache_to_daily_key(
        self,
    ) -> None:
        redis = FakeRedis()
        cache = LiveTimingCache(redis)
        cache_date = date(2026, 11, 1)
        old_topic = f"TimingData:{cache_date}:Practice"
        latest_topic = f"TimingData:{cache_date}:Race"
        redis.values[cache._snapshot_key(old_topic)] = json.dumps(
            {"receivedAt": "2026-11-01T03:00:00+00:00", "payload": {"old": True}}
        )
        latest_snapshot = {
            "receivedAt": "2026-11-01T04:00:00+00:00",
            "payload": {"latest": True},
        }
        redis.values[cache._snapshot_key(latest_topic)] = json.dumps(latest_snapshot)
        daily_key = cache._snapshot_key(f"TimingData:{cache_date}")

        result = await cache.get_snapshot(f"TimingData:{cache_date}")

        self.assertEqual(result, latest_snapshot)
        self.assertEqual(json.loads(redis.values[daily_key]), latest_snapshot)

    async def test_get_snapshot_ignores_legacy_cache_from_another_day(self) -> None:
        redis = FakeRedis()
        cache = LiveTimingCache(redis)
        cache_date = date(2026, 11, 1)
        yesterday_topic = "TimingData:2026-10-31:Race"
        redis.values[cache._snapshot_key(yesterday_topic)] = json.dumps(
            {"receivedAt": "2026-10-31T23:59:00+00:00", "payload": {}}
        )

        self.assertIsNone(await cache.get_snapshot(f"TimingData:{cache_date}"))
