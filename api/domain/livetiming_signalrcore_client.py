import asyncio
import logging
import threading
from collections import defaultdict
from dataclasses import dataclass
from datetime import datetime, timezone
from typing import Any, Callable, DefaultDict

logger = logging.getLogger(__name__)


@dataclass(frozen=True)
class LiveTimingEvent:
    topic: str
    payload: Any
    received_at: datetime
    is_snapshot: bool = False


class LivetimingSignalrcoreClient:
    def __init__(
        self,
        connection_url: str,
        negotiate_url: str,
        access_token_factory: Callable[[], str | None],
        topics: list[str],
        queue_size: int = 100,
    ) -> None:
        self.connection_url = connection_url
        self.negotiate_url = negotiate_url
        self.access_token_factory = access_token_factory
        self.topics = topics
        self.queue_size = queue_size

        self.connection = None
        self._loop: asyncio.AbstractEventLoop | None = None
        self._connect_lock: asyncio.Lock | None = None
        self._connected = False
        self._lock = threading.RLock()
        self._subscribers: DefaultDict[str, set[asyncio.Queue[LiveTimingEvent]]] = (
            defaultdict(set)
        )

    @property
    def is_connected(self) -> bool:
        return self._connected

    async def connect(self) -> None:
        with self._lock:
            if self._connected:
                return

        self._loop = asyncio.get_running_loop()
        await asyncio.to_thread(self._connect_sync)

    async def ensure_connected(self) -> None:
        if not self._connect_lock:
            self._connect_lock = asyncio.Lock()

        async with self._connect_lock:
            with self._lock:
                if self._connected:
                    return

            await self.connect()

    async def disconnect(self) -> None:
        with self._lock:
            connection = self.connection
            self.connection = None
            self._connected = False

        if connection:
            await asyncio.to_thread(connection.stop)

    def subscribe(self, topic: str) -> asyncio.Queue[LiveTimingEvent]:
        queue: asyncio.Queue[LiveTimingEvent] = asyncio.Queue(maxsize=self.queue_size)

        with self._lock:
            self._subscribers[topic].add(queue)

        return queue

    def unsubscribe(self, topic: str, queue: asyncio.Queue[LiveTimingEvent]) -> None:
        with self._lock:
            self._subscribers[topic].discard(queue)

            if not self._subscribers[topic]:
                self._subscribers.pop(topic, None)

    def _connect_sync(self) -> None:
        try:
            import requests
            from signalrcore.hub_connection_builder import HubConnectionBuilder
        except ModuleNotFoundError as exc:
            raise RuntimeError(
                "Live timing dependencies are not installed. "
                "Install project requirements before using live timing endpoints."
            ) from exc

        aws_cookie = self._get_aws_cookie(requests)
        headers = {"Cookie": f"AWSALBCORS={aws_cookie}"} if aws_cookie else {}

        connection_options: dict[str, Any] = {"headers": headers}
        access_token = self.access_token_factory()

        if access_token:
            connection_options["access_token_factory"] = lambda: access_token

        connection = (
            HubConnectionBuilder()
            .with_url(
                self.connection_url,
                options=connection_options,
            )
            .configure_logging(logging.INFO)
            .build()
        )

        connection.on_open(self._on_open)
        connection.on_close(self._on_close)
        connection.on_error(self._on_error)
        connection.on("feed", self._on_feed)

        with self._lock:
            self.connection = connection

        connection.start()

    def _get_aws_cookie(self, requests_module: Any) -> str | None:
        response = requests_module.options(self.negotiate_url, timeout=10)

        if response.status_code == 405:
            logger.info("OPTIONS negotiate returned 405, retrying with POST")
            response = requests_module.post(self.negotiate_url, timeout=10)

        response.raise_for_status()

        return response.cookies.get("AWSALBCORS")

    def _on_open(self) -> None:
        logger.info("Formula 1 SignalRCore connection opened")

        with self._lock:
            connection = self.connection

        if not connection:
            logger.warning("SignalRCore opened without a connection object")
            return

        connection.send(
            "Subscribe",
            [self.topics],
            on_invocation=self._on_subscribe_response,
        )

        with self._lock:
            self._connected = True

    def _on_close(self) -> None:
        logger.warning("Formula 1 SignalRCore connection closed")

        with self._lock:
            self._connected = False

    def _on_error(self, error: Any) -> None:
        logger.error("Formula 1 SignalRCore connection error: %s", error)

    def _on_feed(self, message: Any) -> None:
        try:
            topic, payload = message[0], message[1]
        except (IndexError, TypeError):
            logger.warning("Invalid SignalRCore feed message: %r", message)
            return

        self._dispatch(topic, payload, is_snapshot=False)

    def _on_subscribe_response(self, message: Any) -> None:
        result = getattr(message, "result", None)

        if not isinstance(result, dict):
            logger.warning("Invalid SignalRCore subscribe response: %r", message)
            return

        for topic, payload in result.items():
            self._dispatch(topic, payload, is_snapshot=True)

    def _dispatch(self, topic: str, payload: Any, is_snapshot: bool) -> None:
        event = LiveTimingEvent(
            topic=topic,
            payload=payload,
            received_at=datetime.now(timezone.utc),
            is_snapshot=is_snapshot,
        )

        with self._lock:
            subscribers = list(self._subscribers.get(topic, set()))

        if not subscribers:
            return

        if not self._loop:
            logger.warning("Dropping live timing event without an event loop")
            return

        for queue in subscribers:
            self._loop.call_soon_threadsafe(self._publish, queue, event)

    def _publish(
        self,
        queue: asyncio.Queue[LiveTimingEvent],
        event: LiveTimingEvent,
    ) -> None:
        if queue.full():
            try:
                queue.get_nowait()
            except asyncio.QueueEmpty:
                pass

        queue.put_nowait(event)
