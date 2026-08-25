from fastapi import Depends, Request
from redis.asyncio import Redis

from api.services.live_timing_cache import LiveTimingCache
from api.core.config import Settings, get_settings
from api.domain.livetiming_auth import LivetimingAuthProvider
from api.domain.livetiming_signalrcore_client import LivetimingSignalrcoreClient
from api.domain.ocblacktop_client import OcblacktopClient
from api.services.live_session_service import LiveSessionService
from api.services.race_service import RaceService
from api.services.standings_service import StandingsService
from api.services.timing_service import TimingService


def get_ocblacktop_client(
    settings: Settings = Depends(get_settings),
) -> OcblacktopClient:
    return OcblacktopClient(
        base_url=settings.ocblacktop_api_base_url,
        api_key=settings.ocblacktop_api_key,
    )


def get_formula1_race_service(
    client: OcblacktopClient = Depends(get_ocblacktop_client),
) -> RaceService:
    return RaceService(client=client)


def get_standings_service(
    client: OcblacktopClient = Depends(get_ocblacktop_client),
) -> StandingsService:
    return StandingsService(client=client)


def create_livetiming_signalrcore_client(
    settings: Settings,
) -> LivetimingSignalrcoreClient:
    auth_provider = LivetimingAuthProvider(
        subscription_token=settings.formula1_subscription_token,
        auth_file=settings.livetiming_auth_file,
    )

    return LivetimingSignalrcoreClient(
        connection_url=settings.livetiming_signalr_connection_url,
        negotiate_url=settings.livetiming_signalr_negotiate_url,
        access_token_factory=auth_provider.get_auth_token,
        topics=settings.signalr_topics,
    )


def get_livetiming_signalrcore_client(
    request: Request,
) -> LivetimingSignalrcoreClient:
    return request.app.state.livetiming_signalr_client

def get_redis_client(request: Request) -> Redis:
    return request.app.state.redis


def get_live_timing_cache(
    redis: Redis = Depends(get_redis_client),
) -> LiveTimingCache:
    return LiveTimingCache(redis=redis)


def get_timing_service(
    client: LivetimingSignalrcoreClient = Depends(get_livetiming_signalrcore_client),
    cache: LiveTimingCache = Depends(get_live_timing_cache),
) -> TimingService:
    return TimingService(client=client, cache=cache)


def get_live_session_service(
    client: LivetimingSignalrcoreClient = Depends(get_livetiming_signalrcore_client),
) -> LiveSessionService:
    return LiveSessionService(client=client)
