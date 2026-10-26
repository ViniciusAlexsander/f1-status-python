from collections.abc import AsyncGenerator
from copy import deepcopy
import json
from typing import Any

from api.domain.livetiming_signalrcore_client import LivetimingSignalrcoreClient
from api.services.live_timing_cache import LiveTimingCache


class TimingService:
    def __init__(self, client: LivetimingSignalrcoreClient, cache: LiveTimingCache,) -> None:
        self.client = client
        self.cache = cache

    async def stream_timing_data(self) -> AsyncGenerator[dict[str, Any], None]:
        await self.client.ensure_connected()
        queue = self.client.subscribe("TimingData")
        current_state: dict[str, Any] = {}
        current_session_id: str | None = None
        current_cache_key: str | None = None

        try:
            while True:
                event = await queue.get()
                payload = event.payload

                session_changed = (
                    event.session_id is not None
                    and event.session_id != current_session_id
                )
                cache_key = (
                    f"TimingData:{event.received_at.date()}:{event.session_id}"
                    if event.session_id is not None
                    else None
                )

                if cache_key != current_cache_key:
                    current_session_id = event.session_id
                    current_cache_key = cache_key
                    current_state = {}
                    cached_event = (
                        await self.cache.get_snapshot(cache_key)
                        if cache_key is not None
                        else None
                    )
                    if cached_event is not None:
                        cached_payload = cached_event.get("payload")
                        if isinstance(cached_payload, dict):
                            current_state = deepcopy(cached_payload)
                        yield cached_event

                if isinstance(payload, dict):
                    if event.is_snapshot or (session_changed and not current_state):
                        current_state = deepcopy(payload)
                    else:
                        self._merge_dict(current_state, payload)

                event_data = {
                    "topic": event.topic,
                    "payload": current_state.copy(),
                    "receivedAt": event.received_at.isoformat(),
                    "isSnapshot": event.is_snapshot,
                }

                if cache_key is not None:
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
