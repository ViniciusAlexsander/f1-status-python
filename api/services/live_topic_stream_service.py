from collections.abc import AsyncGenerator
from typing import Any

from api.domain.livetiming_signalrcore_client import LivetimingSignalrcoreClient


class LiveTopicStreamService:
    """Forward SignalR topic payloads without parsing or persisting them."""

    def __init__(self, client: LivetimingSignalrcoreClient) -> None:
        self.client = client

    async def stream_topic(self, topic: str) -> AsyncGenerator[Any, None]:
        queue = self.client.subscribe(topic)

        try:
            await self.client.ensure_connected()

            while True:
                event = await queue.get()
                yield event.payload
        finally:
            self.client.unsubscribe(topic, queue)
