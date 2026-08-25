from collections.abc import AsyncGenerator
from datetime import date, datetime
import json
from typing import Any

from api.domain.livetiming_signalrcore_client import LivetimingSignalrcoreClient
from api.services.live_timing_cache import LiveTimingCache


class TimingService:
    def __init__(self, client: LivetimingSignalrcoreClient, cache: LiveTimingCache,) -> None:
        self.client = client
        self.cache = cache

    async def stream_timing_data(self) -> AsyncGenerator[dict[str, Any], None]:
        today = date.today()
        cache_key = f"TimingData:{today}"

        cached_event = await self.cache.get_snapshot(cache_key)

        if cached_event is not None:
            yield cached_event

        await self.client.ensure_connected()
        queue = self.client.subscribe("TimingData")
        current_state: dict[str, Any] = {}

        if cached_event is not None:
            cached_payload = cached_event.get("payload")

            if isinstance(cached_payload, dict):
                current_state = cached_payload.copy()

        try:
            while True:
                event = await queue.get()
                payload = event.payload

                if isinstance(payload, dict):
                    self._merge_dict(current_state, payload)

                event_data = {
                    "topic": event.topic,
                    "payload": current_state.copy(),
                    "receivedAt": event.received_at.isoformat(),
                    "isSnapshot": event.is_snapshot,
                }

                await self.cache.save_snapshot(cache_key, event_data)

                yield event_data

        finally:
            self.client.unsubscribe("TimingData", queue)

    def _merge_dict(self, target: dict[str, Any], update: dict[str, Any]) -> None:
        for key, value in update.items():
            existing = target.get(key)

            if isinstance(existing, dict) and isinstance(value, dict):
                self._merge_dict(existing, value)
                continue

            if isinstance(value, dict):
                target[key] = value.copy()
                continue

            target[key] = value
