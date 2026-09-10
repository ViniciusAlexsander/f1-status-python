from collections.abc import AsyncGenerator
from datetime import date
from typing import Any

from api.domain.livetiming_signalrcore_client import LivetimingSignalrcoreClient
from api.domain.parsers.utils import normalize_stream_data
from api.services.live_timing_cache import LiveTimingCache


class LiveSessionService:
    def __init__(
            self, 
            client: LivetimingSignalrcoreClient, 
            cache: LiveTimingCache
        ) -> None:
        self.client = client
        self.cache = cache

    async def stream_session_state(self, ) -> AsyncGenerator[dict, None]:
        cache_key = f"SessionData:{date.today()}"
        cached_event = await self.cache.get_snapshot(cache_key)

        if cached_event is not None:
            cached_payload = cached_event.get("payload")
            yield cached_payload

        await self.client.ensure_connected()
        queue = self.client.subscribe("SessionData")

        try:
            while True:
                event = await queue.get()
                payload = event.payload

                if not isinstance(payload, dict):
                    continue

                current_state = self._parse_session_state(payload)

                event_data = {
                    "topic": event.topic,
                    "payload": current_state,
                    "receivedAt": event.received_at.isoformat(),
                    "isSnapshot": event.is_snapshot,
                }

                await self.cache.save_snapshot(cache_key, event_data)

                yield current_state

        finally:
            self.client.unsubscribe("SessionData", queue)

    def _parse_session_state(self, payload: dict[str, Any]) -> dict[str, Any]:
        current_state: dict[str, Any] = {
            "lap": 0,
            "trackStatus": None,
            "sessionStatus": None,
        }

        if "Series" in payload:
            for lap_data in normalize_stream_data(payload["Series"]):
                if "Lap" in lap_data:
                    current_state["lap"] = lap_data["Lap"]

        if "StatusSeries" in payload:
            for status_data in normalize_stream_data(payload["StatusSeries"]):
                if "TrackStatus" in status_data:
                    current_state["trackStatus"] = status_data["TrackStatus"]

                if "SessionStatus" in status_data:
                    current_state["sessionStatus"] = status_data["SessionStatus"]

        return current_state