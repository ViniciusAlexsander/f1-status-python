import asyncio
import json
import logging
from collections.abc import AsyncGenerator
from typing import Any

from fastapi import APIRouter, Depends
from fastapi.responses import StreamingResponse

from api.dependencies import (
    get_live_current_tyres_service,
    get_live_session_service,
    get_live_topic_stream_service,
    get_timing_service,
)
from api.services.live_current_tyres_service import LiveCurrentTyresService
from api.services.live_session_service import LiveSessionService
from api.services.live_topic_stream_service import LiveTopicStreamService
from api.services.timing_service import TimingService

router = APIRouter(prefix="/live-timing", tags=["Live Timing"])
logger = logging.getLogger(__name__)

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


def _topic_stream_response(
    topic: str,
    event_name: str,
    service: LiveTopicStreamService,
) -> StreamingResponse:
    return StreamingResponse(
        _sse_events(event_name, service.stream_topic(topic)),
        media_type="text/event-stream",
    )


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
