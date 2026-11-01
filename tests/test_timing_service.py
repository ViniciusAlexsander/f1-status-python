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
        self.active_session_id: str | None = None

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


class FakeCache:
    def __init__(self) -> None:
        self.snapshots: dict[str, dict] = {}
        self.saved_snapshots: dict[str, dict] = {}

    async def get_snapshot(self, cache_key: str):
        return self.snapshots.get(cache_key)

    async def save_snapshot(self, cache_key: str, data: dict) -> None:
        self.saved_snapshots[cache_key] = data


class TimingServiceTest(unittest.IsolatedAsyncioTestCase):
    async def test_stream_yields_current_session_cache_before_live_updates(self) -> None:
        session_id = "Qualifying:2026-10-30T18:00:00Z"
        cache_key = f"TimingData:{datetime.now(timezone.utc).date()}:{session_id}"
        cached_event = {
            "topic": "TimingData",
            "payload": {"Lines": {"63": {"RacingNumber": "63"}}},
            "receivedAt": datetime.now(timezone.utc).isoformat(),
            "isSnapshot": True,
        }
        client = FakeLiveTimingClient()
        client.active_session_id = session_id
        cache = FakeCache()
        cache.snapshots[cache_key] = cached_event
        service = TimingService(client=client, cache=cache)
        stream = service.stream_timing_data()

        first = await asyncio.wait_for(stream.__anext__(), timeout=1)

        self.assertEqual(first, cached_event)
        self.assertTrue(client.connected)
        self.assertEqual(client.topic, "TimingData")

        await client.queue.put(
            LiveTimingEvent(
                topic="TimingData",
                payload={"Lines": {"63": {"NumberOfLaps": 8}}},
                received_at=datetime.now(timezone.utc),
                session_id=session_id,
            )
        )
        second = await asyncio.wait_for(stream.__anext__(), timeout=1)

        self.assertEqual(
            second["payload"]["Lines"]["63"],
            {"RacingNumber": "63", "NumberOfLaps": 8},
        )

        await stream.aclose()
        self.assertTrue(client.unsubscribed)

    async def test_stream_timing_data_merges_partial_line_updates(self) -> None:
        client = FakeLiveTimingClient()
        service = TimingService(client=client, cache=FakeCache())
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
