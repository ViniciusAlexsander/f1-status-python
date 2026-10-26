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


class FakeCache:
    async def get_snapshot(self, cache_key: str):
        return None

    async def save_snapshot(self, cache_key: str, data: dict) -> None:
        return None


class LiveSessionServiceTest(unittest.IsolatedAsyncioTestCase):
    async def test_stream_session_state_merges_session_data(self) -> None:
        client = FakeLiveTimingClient()
        service = LiveSessionService(client=client, cache=FakeCache())
        stream = service.stream_session_state()

        next_state = asyncio.create_task(stream.__anext__())
        await client.queue.put(
            LiveTimingEvent(
                topic="SessionData",
                payload={
                    "Series": [
                        {"Utc": "2026-10-24T20:46:39.767Z", "QualifyingPart": 1},
                        {"Utc": "2026-10-24T21:23:59.626Z", "QualifyingPart": 2},
                        {"Utc": "2026-10-24T21:46:00.061Z", "QualifyingPart": 3},
                        {"Lap": 12},
                    ],
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
                "qualifyingPart": 3,
            },
        )

        await stream.aclose()

        self.assertTrue(client.unsubscribed)

    def test_parse_session_state_leaves_qualifying_part_empty_when_absent(self) -> None:
        service = LiveSessionService(client=FakeLiveTimingClient(), cache=FakeCache())

        state = service._parse_session_state(
            {"Series": [{"Utc": "2026-10-25T19:06:57.934Z", "Lap": 1}]}
        )

        self.assertIsNone(state["qualifyingPart"])
