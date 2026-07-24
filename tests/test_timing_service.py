import asyncio
import unittest
from datetime import datetime, timezone

from api.domain.livetiming_signalrcore_client import LiveTimingEvent
from api.services.timing_service import TimingService


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
        self.unsubscribed = topic == "TimingData" and queue is self.queue


class TimingServiceTest(unittest.IsolatedAsyncioTestCase):
    async def test_stream_timing_data_merges_partial_line_updates(self) -> None:
        client = FakeLiveTimingClient()
        service = TimingService(client=client)
        stream = service.stream_timing_data()

        next_event = asyncio.create_task(stream.__anext__())
        await client.queue.put(
            LiveTimingEvent(
                topic="TimingData",
                payload={
                    "Lines": {
                        "63": {
                            "NumberOfLaps": 8,
                            "Sectors": {"2": {"Value": "26.907"}},
                        }
                    }
                },
                received_at=datetime.now(timezone.utc),
            )
        )

        first = await asyncio.wait_for(next_event, timeout=1)

        self.assertTrue(client.connected)
        self.assertEqual(
            first["payload"],
            {
                "Lines": {
                    "63": {
                        "NumberOfLaps": 8,
                        "Sectors": {"2": {"Value": "26.907"}},
                    }
                }
            },
        )

        next_event = asyncio.create_task(stream.__anext__())
        await client.queue.put(
            LiveTimingEvent(
                topic="TimingData",
                payload={
                    "Lines": {
                        "63": {
                            "Sectors": {"2": {"PreviousValue": "26.907"}},
                            "Speeds": {"FL": {"Value": "264"}},
                        }
                    }
                },
                received_at=datetime.now(timezone.utc),
            )
        )

        second = await asyncio.wait_for(next_event, timeout=1)

        self.assertEqual(
            second["payload"],
            {
                "Lines": {
                    "63": {
                        "NumberOfLaps": 8,
                        "Sectors": {
                            "2": {
                                "Value": "26.907",
                                "PreviousValue": "26.907",
                            }
                        },
                        "Speeds": {"FL": {"Value": "264"}},
                    }
                }
            },
        )

        await stream.aclose()

        self.assertTrue(client.unsubscribed)
