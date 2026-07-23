import asyncio
import unittest
from datetime import datetime, timezone

from api.domain.livetiming_signalrcore_client import LiveTimingEvent
from api.services.live_session_service import LiveSessionService


class FakeLiveTimingClient:
    def __init__(self) -> None:
        self.queue: asyncio.Queue[LiveTimingEvent] = asyncio.Queue()
        self.connected = False
        self.unsubscribed = False

    async def ensure_connected(self) -> None:
        self.connected = True

    def subscribe(self, topic: str) -> asyncio.Queue[LiveTimingEvent]:
        self.topic = topic

        return self.queue

    def unsubscribe(
        self,
        topic: str,
        queue: asyncio.Queue[LiveTimingEvent],
    ) -> None:
        self.unsubscribed = topic == "SessionData" and queue is self.queue


class LiveSessionServiceTest(unittest.IsolatedAsyncioTestCase):
    async def test_stream_session_state_merges_session_data(self) -> None:
        client = FakeLiveTimingClient()
        service = LiveSessionService(client=client)
        stream = service.stream_session_state()

        next_state = asyncio.create_task(stream.__anext__())
        await client.queue.put(
            LiveTimingEvent(
                topic="SessionData",
                payload={
                    "Series": [{"Lap": 12}],
                    "StatusSeries": [
                        {
                            "TrackStatus": "1",
                            "SessionStatus": "Started",
                        }
                    ],
                },
                received_at=datetime.now(timezone.utc),
            )
        )

        state = await asyncio.wait_for(next_state, timeout=1)

        self.assertTrue(client.connected)
        self.assertEqual(
            state,
            {
                "lap": 12,
                "trackStatus": "1",
                "sessionStatus": "Started",
            },
        )

        await stream.aclose()

        self.assertTrue(client.unsubscribed)
