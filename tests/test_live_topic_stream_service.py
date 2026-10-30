import asyncio
import unittest
from types import SimpleNamespace

from api.domain.livetiming_signalrcore_client import LivetimingSignalrcoreClient
from api.domain.live_topics import RAW_FORWARD_SIGNALR_TOPICS
from api.services.live_topic_stream_service import LiveTopicStreamService
from redis.exceptions import RedisError


class FakeLiveTimingClient:
    def __init__(self) -> None:
        self.queues: dict[str, asyncio.Queue] = {}
        self.unsubscribed: list[tuple[str, asyncio.Queue]] = []
        self.connected = False

    async def ensure_connected(self) -> None:
        self.connected = True

    def subscribe(self, topic: str) -> asyncio.Queue:
        queue = asyncio.Queue()
        self.queues[topic] = queue
        return queue

    def unsubscribe(self, topic: str, queue: asyncio.Queue) -> None:
        self.unsubscribed.append((topic, queue))


class FakeCache:
    def __init__(self, snapshots: dict[str, dict] | None = None) -> None:
        self.snapshots = snapshots or {}

    async def get_snapshot(self, topic: str) -> dict | None:
        return self.snapshots.get(topic)


class UnavailableCache:
    async def get_snapshot(self, topic: str) -> dict | None:
        raise RedisError("Redis unavailable")


class LiveTopicStreamServiceTest(unittest.IsolatedAsyncioTestCase):
    async def test_forwards_any_json_payload_and_unsubscribes(self) -> None:
        payloads = [
            {"nested": {"value": 1}},
            ["a", 2, False],
            "status",
            17,
            None,
        ]

        for topic in RAW_FORWARD_SIGNALR_TOPICS:
            with self.subTest(topic=topic):
                client = FakeLiveTimingClient()
                service = LiveTopicStreamService(client, FakeCache())
                stream = service.stream_topic(topic)

                for payload in payloads:
                    next_item = asyncio.create_task(stream.__anext__())
                    await asyncio.sleep(0)
                    await client.queues[topic].put(SimpleNamespace(payload=payload))
                    self.assertEqual(await asyncio.wait_for(next_item, 1), payload)

                await stream.aclose()
                self.assertTrue(client.connected)
                self.assertEqual(client.unsubscribed, [(topic, client.queues[topic])])

    async def test_track_status_stream_starts_with_cached_latest_payload(self) -> None:
        client = FakeLiveTimingClient()
        service = LiveTopicStreamService(
            client,
            FakeCache(
                {"TrackStatus": {"payload": {"Status": "2", "Message": "Yellow"}}}
            ),
        )
        stream = service.stream_topic("TrackStatus")

        self.assertEqual(
            await asyncio.wait_for(stream.__anext__(), 1),
            {"Status": "2", "Message": "Yellow"},
        )

        next_item = asyncio.create_task(stream.__anext__())
        await asyncio.sleep(0)
        await client.queues["TrackStatus"].put(
            SimpleNamespace(payload={"Status": "1", "Message": "AllClear"})
        )
        self.assertEqual(
            await asyncio.wait_for(next_item, 1),
            {"Status": "1", "Message": "AllClear"},
        )
        await stream.aclose()

    async def test_weather_stream_continues_when_redis_snapshot_is_unavailable(self) -> None:
        client = FakeLiveTimingClient()
        service = LiveTopicStreamService(client, UnavailableCache())
        stream = service.stream_topic("WeatherData")
        next_item = asyncio.create_task(stream.__anext__())
        await asyncio.sleep(0)
        await client.queues["WeatherData"].put(
            SimpleNamespace(payload={"AirTemp": 25})
        )

        self.assertEqual(await asyncio.wait_for(next_item, 1), {"AirTemp": 25})
        await stream.aclose()

    async def test_conditions_stream_shares_weather_and_track_status(self) -> None:
        client = FakeLiveTimingClient()
        service = LiveTopicStreamService(
            client,
            FakeCache(
                {
                    "TrackStatus": {"payload": {"Status": "1"}},
                    "WeatherData": {"payload": {"AirTemp": "23.2"}},
                    "ExtrapolatedClock": {
                        "payload": {"Remaining": "00:20:00"}
                    },
                    "RaceControlMessages": {
                        "payload": {
                            "Messages": [{"Message": "Green", "Utc": "t1"}]
                        }
                    },
                }
            ),
        )
        stream = service.stream_conditions()

        self.assertEqual(
            await asyncio.wait_for(stream.__anext__(), 1),
            ("track_status_update", {"Status": "1"}),
        )
        self.assertEqual(
            await asyncio.wait_for(stream.__anext__(), 1),
            ("weather_data_update", {"AirTemp": "23.2"}),
        )
        self.assertEqual(
            await asyncio.wait_for(stream.__anext__(), 1),
            ("extrapolated_clock_update", {"Remaining": "00:20:00"}),
        )
        self.assertEqual(
            await asyncio.wait_for(stream.__anext__(), 1),
            (
                "race_control_messages_update",
                {"Messages": [{"Message": "Green", "Utc": "t1"}]},
            ),
        )

        next_event = asyncio.create_task(stream.__anext__())
        await asyncio.sleep(0)
        await client.queues["WeatherData"].put(
            SimpleNamespace(raw_payload={"AirTemp": "23.4"}, payload={})
        )
        self.assertEqual(
            await asyncio.wait_for(next_event, 1),
            ("weather_data_update", {"AirTemp": "23.4"}),
        )

        next_event = asyncio.create_task(stream.__anext__())
        await asyncio.sleep(0)
        await client.queues["ExtrapolatedClock"].put(
            SimpleNamespace(raw_payload={"Remaining": "00:12:34"}, payload={})
        )
        self.assertEqual(
            await asyncio.wait_for(next_event, 1),
            ("extrapolated_clock_update", {"Remaining": "00:12:34"}),
        )

        next_event = asyncio.create_task(stream.__anext__())
        await asyncio.sleep(0)
        await client.queues["RaceControlMessages"].put(
            SimpleNamespace(
                raw_payload={"Messages": [{"Message": "Yellow flag"}]},
                payload={},
            )
        )
        self.assertEqual(
            await asyncio.wait_for(next_event, 1),
            (
                "race_control_messages_update",
                {"Messages": [{"Message": "Yellow flag"}]},
            ),
        )

        await stream.aclose()
        self.assertEqual(
            client.unsubscribed,
            [
                ("TrackStatus", client.queues["TrackStatus"]),
                ("WeatherData", client.queues["WeatherData"]),
                ("ExtrapolatedClock", client.queues["ExtrapolatedClock"]),
                ("RaceControlMessages", client.queues["RaceControlMessages"]),
            ],
        )

    async def test_weather_stream_starts_with_cached_latest_payload(self) -> None:
        client = FakeLiveTimingClient()
        weather = {"AirTemp": 25, "TrackTemp": 37}
        service = LiveTopicStreamService(
            client,
            FakeCache({"WeatherData": {"payload": weather}}),
        )
        stream = service.stream_topic("WeatherData")

        self.assertEqual(await asyncio.wait_for(stream.__anext__(), 1), weather)
        await stream.aclose()

    async def test_signalr_client_preserves_raw_topic_updates(self) -> None:
        client = LivetimingSignalrcoreClient(
            connection_url="wss://example.test/signalrcore",
            negotiate_url="https://example.test/signalrcore/negotiate",
            access_token_factory=lambda: None,
            topics=list(RAW_FORWARD_SIGNALR_TOPICS),
        )
        client._loop = asyncio.get_running_loop()
        queue = client.subscribe("SessionInfo")
        first_payload = {"SessionName": "Race"}
        next_payload = {"Meeting": {"Name": "Grand Prix"}}

        client._dispatch("SessionInfo", first_payload, is_snapshot=True)
        await asyncio.sleep(0)
        client._dispatch("SessionInfo", next_payload, is_snapshot=False)

        first = await asyncio.wait_for(queue.get(), 1)
        second = await asyncio.wait_for(queue.get(), 1)
        self.assertEqual(first.payload, first_payload)
        self.assertEqual(second.payload, next_payload)


if __name__ == "__main__":
    unittest.main()
