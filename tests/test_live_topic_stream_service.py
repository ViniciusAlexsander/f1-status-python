import asyncio
import unittest
from types import SimpleNamespace

from api.domain.livetiming_signalrcore_client import LivetimingSignalrcoreClient
from api.domain.live_topics import RAW_FORWARD_SIGNALR_TOPICS
from api.services.live_topic_stream_service import LiveTopicStreamService


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
                service = LiveTopicStreamService(client)
                stream = service.stream_topic(topic)

                for payload in payloads:
                    next_item = asyncio.create_task(stream.__anext__())
                    await asyncio.sleep(0)
                    await client.queues[topic].put(SimpleNamespace(payload=payload))
                    self.assertEqual(await asyncio.wait_for(next_item, 1), payload)

                await stream.aclose()
                self.assertTrue(client.connected)
                self.assertEqual(client.unsubscribed, [(topic, client.queues[topic])])

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
