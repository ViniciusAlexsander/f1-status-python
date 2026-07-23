import asyncio
import json
import logging
from collections.abc import AsyncGenerator
from typing import Any

from fastapi import APIRouter, Depends
from fastapi.responses import StreamingResponse

from api.dependencies import get_live_session_service, get_timing_service
from api.services.live_session_service import LiveSessionService
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


async def _sse_events(
    event_name: str,
    source: AsyncGenerator[dict[str, Any], None],
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

        await source.aclose()
