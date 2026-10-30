import asyncio
from copy import deepcopy
import json
import logging
from typing import Any

from redis.asyncio import Redis

from api.domain.live_topics import REQUIRED_SIGNALR_TOPICS
from api.domain.livetiming_signalrcore_client import (
    LiveTimingEvent,
    LivetimingSignalrcoreClient,
)
from api.services.live_timing_cache import LiveTimingCache

logger = logging.getLogger(__name__)
HISTORY_QUEUE_SIZE = 10000
SNAPSHOT_TOPICS = frozenset(
    {
        "ExtrapolatedClock",
        "RaceControlMessages",
        "TrackStatus",
        "TyreStintSeries",
        "WeatherData",
    }
)


class LiveTopicHistoryService:
    def __init__(self, client: LivetimingSignalrcoreClient, redis: Redis) -> None:
        self.client = client
        self.redis = redis
        self.cache = LiveTimingCache(redis)

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
        if event.topic in SNAPSHOT_TOPICS:
            await self._save_latest_snapshot(event, envelope)

        return await self.redis.xadd(
            self._history_key(event.topic),
            {"event": json.dumps(envelope, separators=(",", ":"))},
        )

    async def _save_latest_snapshot(
        self,
        event: LiveTimingEvent,
        envelope: dict[str, Any],
    ) -> None:
        payload = envelope["payload"]
        previous = await self.cache.get_snapshot(event.topic)
        previous_payload = previous.get("payload") if previous else None

        if (
            event.is_snapshot
            or (isinstance(payload, dict) and payload.get("_kf") is True)
            or previous_payload is None
        ):
            current_payload = deepcopy(payload)
        elif event.topic == "RaceControlMessages":
            current_payload = self._merge_race_control_messages(
                previous_payload, payload
            )
        elif isinstance(previous_payload, dict) and isinstance(payload, dict):
            current_payload = deepcopy(previous_payload)
            self._merge_dict(current_payload, payload)
        else:
            current_payload = deepcopy(payload)

        await self.cache.save_snapshot(
            event.topic,
            {**envelope, "payload": current_payload},
        )

    @staticmethod
    def _merge_race_control_messages(
        current: Any,
        update: Any,
    ) -> Any:
        if not isinstance(current, dict) or not isinstance(update, dict):
            return deepcopy(update)

        merged = deepcopy(current)
        current_messages = current.get("Messages")
        update_messages = update.get("Messages")
        if not isinstance(current_messages, list) or not isinstance(
            update_messages, list
        ):
            LiveTopicHistoryService._merge_dict(merged, update)
            return merged

        messages_by_id = {
            LiveTopicHistoryService._race_control_message_id(message): message
            for message in current_messages
            if isinstance(message, dict)
        }
        for message in update_messages:
            if isinstance(message, dict):
                messages_by_id[
                    LiveTopicHistoryService._race_control_message_id(message)
                ] = message

        metadata_update = {
            key: value for key, value in update.items() if key != "Messages"
        }
        LiveTopicHistoryService._merge_dict(merged, metadata_update)
        merged["Messages"] = sorted(
            messages_by_id.values(),
            key=lambda message: str(message.get("Utc", "")),
        )
        return merged

    @staticmethod
    def _race_control_message_id(message: dict[str, Any]) -> str:
        return f"{message.get('Utc', '')}|{message.get('Message', '')}"

    @classmethod
    def _merge_dict(
        cls,
        current: dict[str, Any],
        update: dict[str, Any],
    ) -> None:
        for key, value in update.items():
            existing = current.get(key)
            if isinstance(existing, dict) and isinstance(value, dict):
                cls._merge_dict(existing, value)
            else:
                current[key] = deepcopy(value)

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
