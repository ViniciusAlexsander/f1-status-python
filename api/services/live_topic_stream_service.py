import asyncio
from collections.abc import AsyncGenerator
import logging
from typing import Any

from api.domain.livetiming_signalrcore_client import LivetimingSignalrcoreClient
from api.services.live_timing_cache import LiveTimingCache
from redis.exceptions import RedisError


logger = logging.getLogger(__name__)
CACHE_SNAPSHOT_TIMEOUT_SECONDS = 2


class LiveTopicStreamService:
    """Forward live topics and restore cached snapshots."""

    def __init__(
        self,
        client: LivetimingSignalrcoreClient,
        cache: LiveTimingCache,
    ) -> None:
        self.client = client
        self.cache = cache

    async def stream_topic(self, topic: str) -> AsyncGenerator[Any, None]:
        queue = self.client.subscribe(topic)

        try:
            await self.client.ensure_connected()
            if topic in {
                "ExtrapolatedClock",
                "RaceControlMessages",
                "TrackStatus",
                "TyreStintSeries",
                "WeatherData",
            }:
                try:
                    cached_event = await asyncio.wait_for(
                        self.cache.get_snapshot(topic),
                        timeout=CACHE_SNAPSHOT_TIMEOUT_SECONDS,
                    )
                except (RedisError, TimeoutError):
                    logger.exception(
                        "Could not load cached live topic snapshot: %s", topic
                    )
                else:
                    if cached_event is not None:
                        yield cached_event.get("payload")

            while True:
                event = await queue.get()
                yield getattr(event, "raw_payload", event.payload)
        finally:
            self.client.unsubscribe(topic, queue)

    async def stream_conditions(self) -> AsyncGenerator[tuple[str, Any], None]:
        topics = {
            "TrackStatus": "track_status_update",
            "WeatherData": "weather_data_update",
            "ExtrapolatedClock": "extrapolated_clock_update",
            "RaceControlMessages": "race_control_messages_update",
        }
        queues = {topic: self.client.subscribe(topic) for topic in topics}
        pending: dict[asyncio.Task[Any], str] = {}

        try:
            await self.client.ensure_connected()
            for topic, queue in queues.items():
                pending[asyncio.create_task(queue.get())] = topic

            for topic, event_name in topics.items():
                try:
                    cached_event = await asyncio.wait_for(
                        self.cache.get_snapshot(topic),
                        timeout=CACHE_SNAPSHOT_TIMEOUT_SECONDS,
                    )
                except (RedisError, TimeoutError):
                    logger.exception(
                        "Could not load cached live topic snapshot: %s", topic
                    )
                else:
                    if cached_event is not None:
                        yield event_name, cached_event.get("payload")

            while pending:
                done, _ = await asyncio.wait(
                    pending,
                    timeout=15,
                    return_when=asyncio.FIRST_COMPLETED,
                )
                if not done:
                    yield "", None
                    continue

                for task in done:
                    topic = pending.pop(task)
                    event = task.result()
                    yield topics[topic], getattr(event, "raw_payload", event.payload)
                    pending[asyncio.create_task(queues[topic].get())] = topic
        finally:
            for task in pending:
                task.cancel()
            await asyncio.gather(*pending, return_exceptions=True)
            for topic, queue in queues.items():
                self.client.unsubscribe(topic, queue)
