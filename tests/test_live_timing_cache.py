import fnmatch
import json
import unittest
from datetime import date

from api.services.live_timing_cache import LiveTimingCache


class FakeRedis:
    def __init__(self) -> None:
        self.values: dict[str, str] = {}
        self.expirations: dict[str, int | None] = {}
        self.expire_calls: dict[str, int] = {}

    async def get(self, key: str) -> str | None:
        return self.values.get(key)

    async def set(self, key: str, value: str, ex: int | None = None) -> None:
        self.values[key] = value
        self.expirations[key] = ex

    async def ttl(self, key: str) -> int:
        return -1 if key in self.values and self.expirations.get(key) is None else 0

    async def expire(self, key: str, seconds: int) -> None:
        self.expire_calls[key] = seconds
        self.expirations[key] = seconds

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
        self.assertEqual(
            redis.expirations[daily_key],
            LiveTimingCache.TIMING_SNAPSHOT_TTL_SECONDS,
        )
        self.assertEqual(
            redis.expire_calls,
            {
                cache._snapshot_key(
                    old_topic
                ): LiveTimingCache.TIMING_SNAPSHOT_TTL_SECONDS,
                cache._snapshot_key(
                    latest_topic
                ): LiveTimingCache.TIMING_SNAPSHOT_TTL_SECONDS,
            },
        )

    async def test_get_snapshot_ignores_legacy_cache_from_another_day(self) -> None:
        redis = FakeRedis()
        cache = LiveTimingCache(redis)
        cache_date = date(2026, 11, 1)
        yesterday_topic = "TimingData:2026-10-31:Race"
        redis.values[cache._snapshot_key(yesterday_topic)] = json.dumps(
            {"receivedAt": "2026-10-31T23:59:00+00:00", "payload": {}}
        )

        self.assertIsNone(await cache.get_snapshot(f"TimingData:{cache_date}"))

    async def test_get_snapshot_adds_expiry_to_existing_daily_cache(self) -> None:
        redis = FakeRedis()
        cache = LiveTimingCache(redis)
        topic = "TimingData:2026-11-01"
        snapshot_key = cache._snapshot_key(topic)
        snapshot = {"payload": {}}
        redis.values[snapshot_key] = json.dumps(snapshot)

        self.assertEqual(await cache.get_snapshot(topic), snapshot)
        self.assertEqual(
            redis.expire_calls[snapshot_key],
            LiveTimingCache.TIMING_SNAPSHOT_TTL_SECONDS,
        )

    async def test_save_snapshot_sets_expiry_only_on_daily_timing_cache(self) -> None:
        redis = FakeRedis()
        cache = LiveTimingCache(redis)

        await cache.save_snapshot(
            "TimingData:2026-11-01",
            {"payload": {}},
        )
        await cache.save_snapshot("SessionData:2026-11-01", {"payload": {}})

        self.assertEqual(
            redis.expirations[cache._snapshot_key("TimingData:2026-11-01")],
            LiveTimingCache.TIMING_SNAPSHOT_TTL_SECONDS,
        )
        self.assertIsNone(
            redis.expirations[cache._snapshot_key("SessionData:2026-11-01")]
        )
