import asyncio
import json
import unittest

from api.core.config import Settings
from api.domain.live_topics import RAW_FORWARD_SIGNALR_TOPICS, REQUIRED_SIGNALR_TOPICS
from api.routers import live_timing


class LiveTopicRouterTest(unittest.IsolatedAsyncioTestCase):
    def test_exposes_an_sse_endpoint_for_each_new_topic(self) -> None:
        paths = {route.path for route in live_timing.router.routes}

        self.assertTrue(
            {
                "/live-timing/session-info",
                "/live-timing/extrapolated-clock",
                "/live-timing/weather-data",
                "/live-timing/race-control-messages",
            }.issubset(paths)
        )

    def test_required_topics_are_added_to_custom_topic_configuration(self) -> None:
        settings = Settings.model_construct(
            livetiming_signalr_topics="CustomTopic, SessionInfo"
        )

        self.assertIn("CustomTopic", settings.signalr_topics)
        self.assertTrue(set(RAW_FORWARD_SIGNALR_TOPICS).issubset(settings.signalr_topics))
        self.assertTrue(set(REQUIRED_SIGNALR_TOPICS).issubset(settings.signalr_topics))

    async def test_sse_serializes_payload_without_changing_its_shape(self) -> None:
        async def source():
            for value in ({"x": 1}, [1, "two"], "text", 9, None):
                yield value

        events = [
            event
            async for event in live_timing._sse_events("topic_update", source())
        ]

        self.assertEqual(
            events,
            [
                f"event: topic_update\ndata: {json.dumps(value)}\n\n"
                for value in ({"x": 1}, [1, "two"], "text", 9, None)
            ],
        )

    async def test_sse_sends_keepalive_and_closes_after_source_error(self) -> None:
        original_keepalive = live_timing.KEEPALIVE_SECONDS
        live_timing.KEEPALIVE_SECONDS = 0.001

        async def source():
            await asyncio.sleep(0.01)
            raise RuntimeError("source failed")
            yield None

        try:
            events = [
                event
                async for event in live_timing._sse_events("topic_update", source())
            ]
        finally:
            live_timing.KEEPALIVE_SECONDS = original_keepalive

        self.assertEqual(events[0], ": keepalive\n\n")
        self.assertEqual(
            events[-1],
            "event: live_timing_error\ndata: {\"detail\": \"source failed\"}\n\n",
        )

    async def test_sse_disconnect_cancels_pending_read_before_closing_source(self) -> None:
        source_started = asyncio.Event()
        source_closed = asyncio.Event()

        async def source():
            source_started.set()
            try:
                await asyncio.Event().wait()
                yield {"unreachable": True}
            finally:
                source_closed.set()

        stream = live_timing._sse_events("topic_update", source())
        pending_read = asyncio.create_task(stream.__anext__())
        await source_started.wait()
        pending_read.cancel()

        with self.assertRaises(asyncio.CancelledError):
            await pending_read

        self.assertTrue(source_closed.is_set())


if __name__ == "__main__":
    unittest.main()
