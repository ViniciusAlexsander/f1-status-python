import json
from collections.abc import AsyncGenerator
from typing import Any

from redis.asyncio import Redis


class LiveTimingCache:
    TIMING_TOPIC = "TimingData"
    SESSION_TOPIC = "SessionData"

    def __init__(self, redis: Redis) -> None:
        self.redis = redis

    def _snapshot_key(self, topic: str) -> str:
        return f"live_timing:snapshot:{topic}"

    def _updates_channel(self, topic: str) -> str:
        return f"live_timing:updates:{topic}"

    async def save_snapshot(self, topic: str, data: dict[str, Any]) -> None:
        encoded = json.dumps(data)

        await self.redis.set(self._snapshot_key(topic), encoded)
        await self.redis.publish(self._updates_channel(topic), encoded)

    async def get_snapshot(self, topic: str) -> dict[str, Any] | None:
        value = await self.redis.get(self._snapshot_key(topic))

        if value is None:
            return None

        if isinstance(value, bytes):
            value = value.decode("utf-8")

        return json.loads(value)

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