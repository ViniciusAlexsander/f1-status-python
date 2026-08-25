from contextlib import asynccontextmanager

import uvicorn
from fastapi import APIRouter, FastAPI
from fastapi.middleware.cors import CORSMiddleware
from redis.asyncio import Redis

from api.dependencies import create_livetiming_signalrcore_client
from api.core.config import get_settings
from api.routers.live_timing import router as live_timing_router
from api.routers.races import router as races_router
from api.routers.standings import router as standings_router


settings = get_settings()


@asynccontextmanager
async def lifespan(app: FastAPI):
    livetiming_client = create_livetiming_signalrcore_client(settings)
    app.state.livetiming_signalr_client = livetiming_client

    redis = Redis.from_url(settings.redis_url, decode_responses=True)
    app.state.redis = redis

    try:
        yield
    finally:
        await redis.aclose()
        await livetiming_client.disconnect()


app = FastAPI(title="F1 Status API", lifespan=lifespan)
api_router_v1 = APIRouter(prefix="/api/v1")

app.add_middleware(
    CORSMiddleware,
    allow_origins=settings.cors_origins,
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

api_router_v1.include_router(races_router)
api_router_v1.include_router(standings_router)
api_router_v1.include_router(live_timing_router)

app.include_router(api_router_v1)


@app.get("/health")
async def health_check() -> dict[str, str]:
    return {"status": "ok"}


if __name__ == "__main__":
    settings = get_settings()
    uvicorn.run("main:app", host="0.0.0.0", port=settings.port)
