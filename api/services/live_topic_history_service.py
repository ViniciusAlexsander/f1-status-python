import asyncio
import json
import logging
from typing import Any

from redis.asyncio import Redis

from api.domain.live_topics import REQUIRED_SIGNALR_TOPICS
from api.domain.livetiming_signalrcore_client import (
    LiveTimingEvent,
    LivetimingSignalrcoreClient,
)

logger = logging.getLogger(__name__)
HISTORY_QUEUE_SIZE = 10000


class LiveTopicHistoryService:
    def __init__(self, client: LivetimingSignalrcoreClient, redis: Redis) -> None:
        self.client = client
        self.redis = redis

    async def collect_forever(self) -> None:
        queues = {
            topic: self.client.subscribe(topic, queue_size=HISTORY_QUEUE_SIZE)
            for topic in REQUIRED_SIGNALR_TOPICS
        }
        retry_delay = 2

        try:
            while True:
                try:
                    await self.client.ensure_connected()
                    retry_delay = 2
                    await asyncio.gather(
                        *(self._record_topic(topic, queue) for topic, queue in queues.items())
                    )
                except asyncio.CancelledError:
                    raise
                except Exception:
                    logger.exception("Live topic history collector failed")
                    await asyncio.sleep(retry_delay)
                    retry_delay = min(retry_delay * 2, 60)
        finally:
            for topic, queue in queues.items():
                self.client.unsubscribe(topic, queue)

    async def _record_topic(self, topic: str, queue: asyncio.Queue[LiveTimingEvent]) -> None:
        while True:
            event = await queue.get()
            retry_delay = 1
            while True:
                try:
                    await self.save_event(event)
                    break
                except asyncio.CancelledError:
                    raise
                except Exception:
                    logger.exception("Could not persist live topic event: %s", topic)
                    await asyncio.sleep(retry_delay)
                    retry_delay = min(retry_delay * 2, 30)

    async def save_event(self, event: LiveTimingEvent) -> str:
        envelope = {
            "topic": event.topic,
            "payload": event.raw_payload,
            "receivedAt": event.received_at.isoformat(),
            "isSnapshot": event.is_snapshot,
            "sessionId": event.session_id,
        }
        return await self.redis.xadd(
            self._history_key(event.topic),
            {"event": json.dumps(envelope, separators=(",", ":"))},
        )

    async def get_history(
        self, topic: str, limit: int = 100, before: str | None = None
    ) -> dict[str, Any]:
        max_id = f"({before}" if before else "+"
        rows = await self.redis.xrevrange(
            self._history_key(topic), max=max_id, min="-", count=limit
        )
        events = []
        for stream_id, fields in rows:
            raw = fields.get("event")
            if raw is None:
                continue
            events.append({"id": stream_id, **json.loads(raw)})

        return {
            "topic": topic,
            "events": events,
            "total": await self.redis.xlen(self._history_key(topic)),
            "nextBefore": events[-1]["id"] if len(events) == limit else None,
        }

    @staticmethod
    def _history_key(topic: str) -> str:
        return f"live_timing:history:{topic}"
