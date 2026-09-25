from collections.abc import AsyncGenerator
from datetime import date
from typing import Any

from api.domain.livetiming_signalrcore_client import LivetimingSignalrcoreClient
from api.services.live_timing_cache import LiveTimingCache


TOPIC = "CurrentTyres"

class LiveCurrentTyresService:
    def __init__(
            self, 
            livetimingSignalrcoreClient: LivetimingSignalrcoreClient, 
            cache: LiveTimingCache
        ) -> None:
        self.livetimingSignalrcoreClient = livetimingSignalrcoreClient
        self.cache = cache

    async def stream_current_tyres_state(self, ) -> AsyncGenerator[dict, None]:
        cache_key = f"{TOPIC}:{date.today()}"
        cached_event = await self.cache.get_snapshot(cache_key)

        if cached_event is not None:
            cached_payload = cached_event.get("payload")
            yield cached_payload

        await self.livetimingSignalrcoreClient.ensure_connected()
        queue = self.livetimingSignalrcoreClient.subscribe(TOPIC)

        try:
            while True:
                event = await queue.get()
                payload = event.payload

                if not isinstance(payload, dict):
                    continue

                current_state = payload

                event_data = {
                    "topic": event.topic,
                    "payload": current_state,
                    "receivedAt": event.received_at.isoformat(),
                    "isSnapshot": event.is_snapshot,
                }

                await self.cache.save_snapshot(cache_key, event_data)

                yield current_state

        finally:
            self.livetimingSignalrcoreClient.unsubscribe(TOPIC, queue)