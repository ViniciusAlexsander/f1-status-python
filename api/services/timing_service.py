from collections.abc import AsyncGenerator
from typing import Any

from api.domain.livetiming_signalrcore_client import LivetimingSignalrcoreClient


class TimingService:
    def __init__(self, client: LivetimingSignalrcoreClient) -> None:
        self.client = client

    async def stream_timing_data(self) -> AsyncGenerator[dict[str, Any], None]:
        await self.client.ensure_connected()
        queue = self.client.subscribe("TimingData")

        try:
            while True:
                event = await queue.get()

                yield {
                    "topic": event.topic,
                    "payload": event.payload,
                    "receivedAt": event.received_at.isoformat(),
                    "isSnapshot": event.is_snapshot,
                }

        finally:
            self.client.unsubscribe("TimingData", queue)
