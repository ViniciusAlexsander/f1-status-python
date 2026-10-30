import asyncio
import json
import logging
from collections.abc import AsyncGenerator
from typing import Any

from fastapi import APIRouter, Depends, HTTPException, Query
from fastapi.responses import StreamingResponse

from api.dependencies import (
    get_live_current_tyres_service,
    get_live_session_service,
    get_live_topic_stream_service,
    get_live_topic_history_service,
    get_timing_service,
)
from api.services.live_current_tyres_service import LiveCurrentTyresService
from api.services.live_session_service import LiveSessionService
from api.services.live_topic_stream_service import LiveTopicStreamService
from api.services.live_topic_history_service import LiveTopicHistoryService
from api.domain.live_topics import REQUIRED_SIGNALR_TOPICS
from api.domain.livetiming_signalrcore_client import LivetimingSignalrcoreClient
from api.dependencies import get_livetiming_signalrcore_client
from api.services.timing_service import TimingService

router = APIRouter(prefix="/live-timing", tags=["Live Timing"])
logger = logging.getLogger(__name__)
LIVE_TOPIC_NAMES = frozenset(REQUIRED_SIGNALR_TOPICS)

KEEPALIVE_SECONDS = 15


@router.get("/timing")
async def stream_timing_data(
    service: TimingService = Depends(get_timing_service),
) -> StreamingResponse:
    return StreamingResponse(
        _sse_events("timing_update", service.stream_timing_data()),
        media_type="text/event-stream",
    )


@router.get("/session")
async def stream_session_state(
    service: LiveSessionService = Depends(get_live_session_service),
) -> StreamingResponse:
    return StreamingResponse(
        _sse_events("session_update", service.stream_session_state()),
        media_type="text/event-stream",
    )

@router.get("/current-tyres")
async def stream_current_tyres_state(
    service: LiveCurrentTyresService = Depends(get_live_current_tyres_service),
) -> StreamingResponse:
    return StreamingResponse(
        _sse_events("current_tyres_update", service.stream_current_tyres_state()),
        media_type="text/event-stream",
    )


@router.get("/session-info")
async def stream_session_info(
    service: LiveTopicStreamService = Depends(get_live_topic_stream_service),
) -> StreamingResponse:
    return _topic_stream_response("SessionInfo", "session_info_update", service)


@router.get("/extrapolated-clock")
async def stream_extrapolated_clock(
    service: LiveTopicStreamService = Depends(get_live_topic_stream_service),
) -> StreamingResponse:
    return _topic_stream_response(
        "ExtrapolatedClock", "extrapolated_clock_update", service
    )


@router.get("/weather-data")
async def stream_weather_data(
    service: LiveTopicStreamService = Depends(get_live_topic_stream_service),
) -> StreamingResponse:
    return _topic_stream_response("WeatherData", "weather_data_update", service)


@router.get("/race-control-messages")
async def stream_race_control_messages(
    service: LiveTopicStreamService = Depends(get_live_topic_stream_service),
) -> StreamingResponse:
    return _topic_stream_response(
        "RaceControlMessages", "race_control_messages_update", service
    )


@router.get("/topics/{topic}/stream")
async def stream_topic(
    topic: str,
    service: LiveTopicStreamService = Depends(get_live_topic_stream_service),
) -> StreamingResponse:
    _validate_topic(topic)
    return _topic_stream_response(topic, "topic_update", service)


@router.get("/topics/status")
async def get_topics_status(
    client: LivetimingSignalrcoreClient = Depends(get_livetiming_signalrcore_client),
) -> dict[str, Any]:
    return {"connected": client.is_connected, "topics": list(REQUIRED_SIGNALR_TOPICS)}


@router.get("/topics/{topic}/history")
async def get_topic_history(
    topic: str,
    limit: int = Query(default=100, ge=1, le=1000),
    before: str | None = Query(default=None),
    service: LiveTopicHistoryService = Depends(get_live_topic_history_service),
) -> dict[str, Any]:
    _validate_topic(topic)
    return await service.get_history(topic, limit=limit, before=before)


def _validate_topic(topic: str) -> None:
    if topic not in LIVE_TOPIC_NAMES:
        raise HTTPException(status_code=404, detail="Unknown live timing topic")


def _topic_stream_response(
    topic: str,
    event_name: str,
    service: LiveTopicStreamService,
) -> StreamingResponse:
    return StreamingResponse(
        _sse_events(event_name, service.stream_topic(topic)),
        media_type="text/event-stream",
    )


@router.get("/conditions")
async def stream_track_conditions(
    service: LiveTopicStreamService = Depends(get_live_topic_stream_service),
) -> StreamingResponse:
    return StreamingResponse(
        _sse_topic_events(service.stream_conditions()),
        media_type="text/event-stream",
    )


async def _sse_topic_events(
    source: AsyncGenerator[tuple[str, Any], None],
) -> AsyncGenerator[str, None]:
    try:
        async for event_name, payload in source:
            if not event_name:
                yield ": keepalive\n\n"
                continue
            yield f"event: {event_name}\ndata: {json.dumps(payload)}\n\n"
    except asyncio.CancelledError:
        raise
    except Exception as exc:
        logger.exception("Live timing conditions SSE stream failed")
        yield (
            "event: live_timing_error\n"
            f"data: {json.dumps({'detail': str(exc)})}\n\n"
        )
    finally:
        await source.aclose()


async def _sse_events(
    event_name: str,
    source: AsyncGenerator[Any, None],
) -> AsyncGenerator[str, None]:
    next_event = asyncio.create_task(source.__anext__())

    try:
        while True:
            done, _ = await asyncio.wait(
                {next_event},
                timeout=KEEPALIVE_SECONDS,
            )

            if not done:
                yield ": keepalive\n\n"
                continue

            try:
                payload = next_event.result()
            except StopAsyncIteration:
                return
            except Exception as exc:
                logger.exception("Live timing SSE stream failed")
                yield (
                    "event: live_timing_error\n"
                    f"data: {json.dumps({'detail': str(exc)})}\n\n"
                )
                return

            yield f"event: {event_name}\ndata: {json.dumps(payload)}\n\n"
            next_event = asyncio.create_task(source.__anext__())

    finally:
        if not next_event.done():
            next_event.cancel()

        await asyncio.gather(next_event, return_exceptions=True)

        if not source.ag_running:
            await source.aclose()
