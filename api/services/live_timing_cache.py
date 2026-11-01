import json
from collections.abc import AsyncGenerator
from datetime import date, datetime, timezone
import logging
from typing import Any

from redis.asyncio import Redis


logger = logging.getLogger(__name__)


class LiveTimingCache:
    TIMING_TOPIC = "TimingData"
    SESSION_TOPIC = "SessionData"
    TIMING_SNAPSHOT_TTL_SECONDS = 3 * 60 * 60

    def __init__(self, redis: Redis) -> None:
        self.redis = redis

    def _snapshot_key(self, topic: str) -> str:
        return f"live_timing:snapshot:{topic}"

    def _updates_channel(self, topic: str) -> str:
        return f"live_timing:updates:{topic}"

    async def save_snapshot(self, topic: str, data: dict[str, Any]) -> None:
        encoded = json.dumps(data)

        snapshot_key = self._snapshot_key(topic)
        if self._daily_timing_cache_date(topic) is not None:
            await self.redis.set(
                snapshot_key,
                encoded,
                ex=self.TIMING_SNAPSHOT_TTL_SECONDS,
            )
        else:
            await self.redis.set(snapshot_key, encoded)
        await self.redis.publish(self._updates_channel(topic), encoded)

    async def get_snapshot(self, topic: str) -> dict[str, Any] | None:
        snapshot_key = self._snapshot_key(topic)
        value = await self.redis.get(snapshot_key)

        if value is None:
            cache_date = self._daily_timing_cache_date(topic)
            if cache_date is None:
                return None

            return await self._migrate_legacy_timing_snapshot(
                topic,
                cache_date,
            )

        if self._daily_timing_cache_date(topic) is not None:
            ttl = await self.redis.ttl(snapshot_key)
            if ttl == -1:
                await self.redis.expire(
                    snapshot_key,
                    self.TIMING_SNAPSHOT_TTL_SECONDS,
                )

        if isinstance(value, bytes):
            value = value.decode("utf-8")

        return json.loads(value)

    def _daily_timing_cache_date(self, topic: str) -> date | None:
        prefix = f"{self.TIMING_TOPIC}:"
        if not topic.startswith(prefix):
            return None

        try:
            return date.fromisoformat(topic.removeprefix(prefix))
        except ValueError:
            return None

    async def _migrate_legacy_timing_snapshot(
        self,
        daily_topic: str,
        cache_date: date,
    ) -> dict[str, Any] | None:
        prefix = f"{self._snapshot_key(daily_topic)}:"
        latest_snapshot: dict[str, Any] | None = None
        latest_timestamp: datetime | None = None
        legacy_keys: list[str] = []

        async for raw_key in self.redis.scan_iter(match=f"{prefix}*"):
            key = raw_key.decode("utf-8") if isinstance(raw_key, bytes) else raw_key
            legacy_keys.append(key)
            value = await self.redis.get(key)
            if value is None:
                continue

            if isinstance(value, bytes):
                value = value.decode("utf-8")
            snapshot = json.loads(value)
            if not isinstance(snapshot, dict):
                logger.warning("Cached timing snapshot is not an object: %s", key)
                continue

            received_at = snapshot.get("receivedAt")
            if not isinstance(received_at, str):
                logger.warning("Cached timing snapshot has no receivedAt: %s", key)
                continue

            try:
                timestamp = datetime.fromisoformat(received_at)
            except ValueError:
                logger.warning("Cached timing snapshot has invalid receivedAt: %s", key)
                continue

            if timestamp.tzinfo is None:
                timestamp = timestamp.replace(tzinfo=timezone.utc)

            if latest_timestamp is None or timestamp > latest_timestamp:
                latest_snapshot = snapshot
                latest_timestamp = timestamp

        if latest_snapshot is not None:
            await self.redis.set(
                self._snapshot_key(daily_topic),
                json.dumps(latest_snapshot),
                ex=self.TIMING_SNAPSHOT_TTL_SECONDS,
            )
            for key in legacy_keys:
                await self.redis.expire(key, self.TIMING_SNAPSHOT_TTL_SECONDS)
            logger.info(
                "Migrated legacy TimingData cache to daily key for %s",
                cache_date,
            )

        return latest_snapshot

    async def stream_updates(
        self,
        topic: str,
    ) -> AsyncGenerator[dict[str, Any], None]:
        pubsub = self.redis.pubsub()
        channel = self._updates_channel(topic)

        await pubsub.subscribe(channel)

        try:
            async for message in pubsub.listen():
                if message["type"] != "message":
                    continue

                data = message["data"]

                if isinstance(data, bytes):
                    data = data.decode("utf-8")

                yield json.loads(data)

        finally:
            await pubsub.unsubscribe(channel)
            await pubsub.close()
