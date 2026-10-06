import copy
import asyncio
import logging
import threading
from collections import defaultdict
from dataclasses import dataclass
from datetime import datetime, timezone
from typing import Any, Callable, DefaultDict

logger = logging.getLogger(__name__)

HANDSHAKE_TIMEOUT_SECONDS = 30.0


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
        handshake_timeout_seconds: float = HANDSHAKE_TIMEOUT_SECONDS,
        reconnect_initial_seconds: float = 2.0,
        reconnect_max_seconds: float = 60.0,
    ) -> None:
        self.connection_url = connection_url
        self.negotiate_url = negotiate_url
        self.access_token_factory = access_token_factory
        self.topics = topics
        self.queue_size = queue_size
        self.handshake_timeout_seconds = handshake_timeout_seconds
        self.reconnect_initial_seconds = reconnect_initial_seconds
        self.reconnect_max_seconds = reconnect_max_seconds

        self.connection = None
        self._loop: asyncio.AbstractEventLoop | None = None
        self._connect_lock = asyncio.Lock()
        self._connected = False
        self._stopping = False
        self._ready: asyncio.Future[None] | None = None
        self._reconnect_task: asyncio.Task[None] | None = None
        self._reconnect_delay = reconnect_initial_seconds
        self._lock = threading.RLock()
        self._topic_state: DefaultDict[str, Any] = defaultdict(dict)
        self._subscribers: DefaultDict[str, set[asyncio.Queue[LiveTimingEvent]]] = (
            defaultdict(set)
        )

    @property
    def is_connected(self) -> bool:
        return self._connected

    async def connect(self) -> None:
        await self.ensure_connected()

    async def ensure_connected(self) -> None:
        if self._stopping:
            raise RuntimeError("SignalRCore client is stopping")

        async with self._connect_lock:
            if self._connected:
                return

            self._loop = asyncio.get_running_loop()

            if self._ready is None or self._ready.done():
                self._ready = self._loop.create_future()
                try:
                    await asyncio.to_thread(self._connect_sync)
                except Exception as exc:
                    self._set_ready_exception(exc)
                    raise

            ready = self._ready

        try:
            await asyncio.wait_for(
                asyncio.shield(ready),
                timeout=self.handshake_timeout_seconds,
            )
        except asyncio.TimeoutError as exc:
            logger.error("SignalRCore handshake timed out")
            await self._stop_current_connection()
            timeout_error = TimeoutError("SignalRCore handshake timed out")
            self._set_ready_exception(timeout_error)
            raise timeout_error from exc

    async def disconnect(self) -> None:
        with self._lock:
            self._stopping = True
            connection = self.connection
            self.connection = None
            self._connected = False
            self._topic_state.clear()

        reconnect_task = self._reconnect_task
        self._reconnect_task = None

        if reconnect_task and not reconnect_task.done():
            reconnect_task.cancel()
            try:
                await reconnect_task
            except asyncio.CancelledError:
                pass

        if self._ready is not None and not self._ready.done():
            self._ready.cancel()

        if connection:
            await asyncio.to_thread(self._stop_hub, connection)

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

        previous = None
        with self._lock:
            previous = self.connection
            self.connection = None

        if previous:
            self._stop_hub(previous)

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
            .build()
        )

        connection.on_open(lambda: self._on_open(connection))
        connection.on_close(lambda: self._on_close(connection))
        connection.on_error(lambda error: self._on_error(connection, error))
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

    def _on_open(self, connection: Any) -> None:
        logger.info("Formula 1 SignalRCore connection opened")

        with self._lock:
            if self._stopping or self.connection is not connection:
                logger.warning("Ignoring SignalRCore open for a stale connection")
                return

        connection.send(
            "Subscribe",
            [self.topics],
            on_invocation=self._on_subscribe_response,
        )

        with self._lock:
            if self.connection is not connection:
                return

            self._connected = True
            self._reconnect_delay = self.reconnect_initial_seconds

        self._set_ready_result()

    def _on_close(self, connection: Any) -> None:
        with self._lock:
            if self.connection is not None and self.connection is not connection:
                return

            logger.warning("Formula 1 SignalRCore connection closed")
            self._connected = False

            if self.connection is connection:
                self.connection = None

            stopping = self._stopping
            has_subscribers = any(self._subscribers.values())

        if stopping:
            return

        self._set_ready_exception(ConnectionError("SignalRCore connection closed"))

        if has_subscribers:
            self._schedule_reconnect()

    def _on_error(self, connection: Any, error: Any) -> None:
        logger.error("Formula 1 SignalRCore connection error: %s", error)

        with self._lock:
            if self.connection is not connection:
                return

            self._connected = False
            self.connection = None
            stopping = self._stopping
            has_subscribers = any(self._subscribers.values())

        self._set_ready_exception(
            ConnectionError(f"SignalRCore connection error: {error}")
        )

        self._stop_hub(connection)

        if not stopping and has_subscribers:
            self._schedule_reconnect()

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
        resolved_payload = self._update_topic_state(topic, payload, is_snapshot)
        event = LiveTimingEvent(
            topic=topic,
            payload=resolved_payload,
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

    def _update_topic_state(
        self,
        topic: str,
        payload: Any,
        is_snapshot: bool,
    ) -> Any:
        if not isinstance(payload, dict):
            return payload

        with self._lock:
            if is_snapshot or topic not in self._topic_state:
                self._topic_state[topic] = copy.deepcopy(payload)
            else:
                self._merge_dict(self._topic_state[topic], payload)

            return copy.deepcopy(self._topic_state[topic])

    def _merge_dict(self, target: dict[str, Any], update: dict[str, Any]) -> None:
        for key, value in update.items():
            existing = target.get(key)

            if isinstance(existing, dict) and isinstance(value, dict):
                self._merge_dict(existing, value)
                continue

            if isinstance(value, dict):
                target[key] = copy.deepcopy(value)
                continue

            target[key] = value

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

    def _has_subscribers(self) -> bool:
        with self._lock:
            return any(self._subscribers.values())

    def _schedule_reconnect(self) -> None:
        loop = self._loop

        if loop is None:
            return

        def start_reconnect() -> None:
            if self._stopping or self._connected:
                return

            if self._reconnect_task is not None and not self._reconnect_task.done():
                return

            if not self._has_subscribers():
                return

            self._reconnect_task = loop.create_task(self._reconnect())

        loop.call_soon_threadsafe(start_reconnect)

    async def _reconnect(self) -> None:
        delay = self._reconnect_delay
        logger.info("Reconnecting to Formula 1 SignalRCore in %.1fs", delay)
        await asyncio.sleep(delay)

        if self._stopping or self._connected or not self._has_subscribers():
            return

        try:
            await self.ensure_connected()
            self._reconnect_delay = self.reconnect_initial_seconds
        except Exception:
            logger.exception("Formula 1 SignalRCore reconnect failed")
            self._reconnect_delay = min(
                self._reconnect_delay * 2,
                self.reconnect_max_seconds,
            )
            self._schedule_reconnect()

    def _set_ready_result(self) -> None:
        loop = self._loop

        if loop is None:
            return

        def complete() -> None:
            if self._ready is None or self._ready.done():
                return

            self._ready.set_result(None)

        loop.call_soon_threadsafe(complete)

    def _set_ready_exception(self, error: BaseException) -> None:
        loop = self._loop

        if loop is None:
            if self._ready is not None and not self._ready.done():
                self._ready.set_exception(error)
            return

        def complete() -> None:
            if self._ready is None or self._ready.done():
                return

            self._ready.set_exception(error)

        try:
            running = asyncio.get_running_loop()
        except RuntimeError:
            running = None

        if running is loop:
            complete()
        else:
            loop.call_soon_threadsafe(complete)

    async def _stop_current_connection(self) -> None:
        with self._lock:
            connection = self.connection
            self.connection = None
            self._connected = False

        if connection:
            await asyncio.to_thread(self._stop_hub, connection)

    @staticmethod
    def _stop_hub(connection: Any) -> None:
        try:
            connection.stop()
        except Exception:
            logger.warning("Failed to stop SignalRCore connection", exc_info=True)
