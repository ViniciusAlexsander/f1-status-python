import asyncio
import unittest
from types import SimpleNamespace

from api.domain.livetiming_signalrcore_client import LivetimingSignalrcoreClient


class LivetimingSignalrcoreClientTest(unittest.IsolatedAsyncioTestCase):
    def create_client(self, queue_size: int = 100) -> LivetimingSignalrcoreClient:
        client = LivetimingSignalrcoreClient(
            connection_url="wss://example.test/signalrcore",
            negotiate_url="https://example.test/signalrcore/negotiate",
            access_token_factory=lambda: "token",
            topics=["TimingData", "SessionData"],
            queue_size=queue_size,
        )
        client._loop = asyncio.get_running_loop()

        return client

    async def test_dispatches_event_to_multiple_subscribers(self) -> None:
        client = self.create_client()
        first = client.subscribe("TimingData")
        second = client.subscribe("TimingData")

        client._dispatch("TimingData", {"Lines": {}}, is_snapshot=False)

        first_event = await asyncio.wait_for(first.get(), timeout=1)
        second_event = await asyncio.wait_for(second.get(), timeout=1)

        self.assertEqual(first_event.payload, {"Lines": {}})
        self.assertEqual(second_event.payload, {"Lines": {}})
        self.assertFalse(first_event.is_snapshot)

    async def test_unsubscribe_removes_only_selected_subscriber(self) -> None:
        client = self.create_client()
        removed = client.subscribe("TimingData")
        active = client.subscribe("TimingData")

        client.unsubscribe("TimingData", removed)
        client._dispatch("TimingData", {"Lines": {"1": {}}}, is_snapshot=False)

        active_event = await asyncio.wait_for(active.get(), timeout=1)

        self.assertEqual(active_event.payload, {"Lines": {"1": {}}})
        self.assertTrue(removed.empty())

    async def test_subscribe_response_uses_dispatch_path(self) -> None:
        client = self.create_client()
        queue = client.subscribe("SessionData")
        message = SimpleNamespace(result={"SessionData": {"Series": []}})

        client._on_subscribe_response(message)

        event = await asyncio.wait_for(queue.get(), timeout=1)

        self.assertEqual(event.topic, "SessionData")
        self.assertEqual(event.payload, {"Series": []})
        self.assertTrue(event.is_snapshot)

    async def test_full_queue_drops_oldest_event(self) -> None:
        client = self.create_client(queue_size=1)
        queue = client.subscribe("TimingData")

        client._dispatch("TimingData", {"value": 1}, is_snapshot=False)
        await asyncio.sleep(0)
        client._dispatch("TimingData", {"value": 2}, is_snapshot=False)
        await asyncio.sleep(0)

        event = await asyncio.wait_for(queue.get(), timeout=1)

        self.assertEqual(event.payload, {"value": 2})
        self.assertTrue(queue.empty())

    async def test_ensure_connected_connects_only_once(self) -> None:
        client = self.create_client()
        calls = 0

        async def connect() -> None:
            nonlocal calls
            calls += 1
            client._connected = True

        client.connect = connect

        await asyncio.gather(
            client.ensure_connected(),
            client.ensure_connected(),
        )

        self.assertEqual(calls, 1)

    async def test_get_aws_cookie_retries_with_post_after_options_405(self) -> None:
        client = self.create_client()
        calls = []

        class Response:
            def __init__(self, status_code: int, cookie: str | None = None) -> None:
                self.status_code = status_code
                self.cookies = {"AWSALBCORS": cookie} if cookie else {}

            def raise_for_status(self) -> None:
                if self.status_code >= 400:
                    raise AssertionError("unexpected status")

        class Requests:
            def options(self, url: str, timeout: int) -> Response:
                calls.append(("OPTIONS", url, timeout))
                return Response(405)

            def post(self, url: str, timeout: int) -> Response:
                calls.append(("POST", url, timeout))
                return Response(200, "cookie-value")

        cookie = client._get_aws_cookie(Requests())

        self.assertEqual(cookie, "cookie-value")
        self.assertEqual(
            calls,
            [
                ("OPTIONS", client.negotiate_url, 10),
                ("POST", client.negotiate_url, 10),
            ],
        )

    async def test_connection_options_omit_access_token_factory_without_token(
        self,
    ) -> None:
        client = self.create_client()
        client.access_token_factory = lambda: None

        class Response:
            status_code = 200
            cookies = {}

            def raise_for_status(self) -> None:
                return None

        class Requests:
            def options(self, url: str, timeout: int) -> Response:
                return Response()

        captured_options = {}

        class Connection:
            def on_open(self, callback):
                return None

            def on_close(self, callback):
                return None

            def on_error(self, callback):
                return None

            def on(self, event, callback):
                return None

            def start(self):
                return None

        class Builder:
            def with_url(self, url: str, options: dict) -> "Builder":
                captured_options.update(options)
                return self

            def configure_logging(self, level: int) -> "Builder":
                return self

            def build(self) -> Connection:
                return Connection()

        original_get_aws_cookie = client._get_aws_cookie
        client._get_aws_cookie = lambda requests_module: None

        import builtins

        real_import = builtins.__import__

        def fake_import(name, globals=None, locals=None, fromlist=(), level=0):
            if name == "requests":
                return Requests()

            if name == "signalrcore.hub_connection_builder":
                return SimpleNamespace(HubConnectionBuilder=Builder)

            return real_import(name, globals, locals, fromlist, level)

        try:
            builtins.__import__ = fake_import
            client._connect_sync()
        finally:
            builtins.__import__ = real_import
            client._get_aws_cookie = original_get_aws_cookie

        self.assertEqual(captured_options, {"headers": {}})
