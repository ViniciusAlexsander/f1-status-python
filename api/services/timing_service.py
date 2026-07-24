from collections.abc import AsyncGenerator
from typing import Any

from api.domain.livetiming_signalrcore_client import LivetimingSignalrcoreClient


class TimingService:
    def __init__(self, client: LivetimingSignalrcoreClient) -> None:
        self.client = client

    async def stream_timing_data(self) -> AsyncGenerator[dict[str, Any], None]:
        await self.client.ensure_connected()
        queue = self.client.subscribe("TimingData")
        current_state: dict[str, Any] = {}

        try:
            while True:
                event = await queue.get()
                payload = event.payload

                if isinstance(payload, dict):
                    self._merge_dict(current_state, payload)

                yield {
                    "topic": event.topic,
                    "payload": current_state.copy(),
                    "receivedAt": event.received_at.isoformat(),
                    "isSnapshot": event.is_snapshot,
                }

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
