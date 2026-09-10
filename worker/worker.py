import asyncio
import logging

from redis.asyncio import Redis

from api.core.config import get_settings
from api.dependencies import create_livetiming_signalrcore_client
from api.domain.ocblacktop_client import OcblacktopClient
from api.schemas.formula1_events import Race
from api.services.live_session_service import LiveSessionService
from api.services.live_timing_cache import LiveTimingCache
from api.services.race_service import RaceService
from api.services.timing_service import TimingService

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)


async def run_worker() -> None:
    logger.info("Worker iniciado")
    settings = get_settings()

    race_service = RaceService(
        client=OcblacktopClient(
            base_url=settings.ocblacktop_api_base_url,
            api_key=settings.ocblacktop_api_key,
        )
    )

    signalr_client = create_livetiming_signalrcore_client(settings)
    redis = Redis.from_url(settings.redis_url, decode_responses=True)
    cache = LiveTimingCache(redis)
    timing_service = TimingService(
        client=signalr_client,
        cache=cache,
    )
    session_service = LiveSessionService(
        client=signalr_client,
        cache=cache,
    )

    live_timing_task: asyncio.Task | None = None
    live_session_task: asyncio.Task | None = None

    try:
        while True:
            try:
                races = await race_service.list_races()
            except Exception:
                logger.exception("Erro ao buscar corridas")
                await asyncio.sleep(60)
                continue
            current_race = races.data.currentRace

            if current_race and have_ongoing_session(current_race):
                if live_timing_task is None or live_timing_task.done():
                    logger.info("Iniciando stream de live timing")
                    live_timing_task = create_stream_task(
                        start_save_live_timing(timing_service),
                        "live-timing",
                    )

                if live_session_task is None or live_session_task.done():
                    live_session_task = create_stream_task(
                        start_save_live_session(session_service),
                        "live-session",
                    )
                   
            else:
                logger.info("Nenhum sessão em andamento. Encerrando streams de live timing")

                await cancel_tasks(live_timing_task, live_session_task)
                live_timing_task = None
                live_session_task = None
    
            await asyncio.sleep(60)
    finally:
        await cancel_tasks(live_timing_task, live_session_task)
        await signalr_client.disconnect()
        await redis.aclose()

def main() -> None:
    try:
        asyncio.run(run_worker())
    except KeyboardInterrupt:
        logger.info("Worker encerrado")

def have_ongoing_session(onGoingWeekend: Race) -> bool:
    for scheduled_session in onGoingWeekend.schedule:
        if scheduled_session.status == "ongoing":
            return True
    return False
        
async def start_save_live_timing(timing_service: TimingService) -> None:
    try:
        async for event in timing_service.stream_timing_data():
            logger.info("Atualização de timing recebida: %s", event["receivedAt"])
    except asyncio.CancelledError:
        logger.info("Stream de live timing cancelado")
        raise

async def start_save_live_session(
    session_service: LiveSessionService,
) -> None:
    try:
        async for event in session_service.stream_session_state():
            logger.info("Estado da sessão recebido: %s", event)
    except asyncio.CancelledError:
        logger.info("Stream da sessão cancelado")
        raise

def log_task_result(task: asyncio.Task, task_name: str) -> None:
    if task.cancelled():
        logger.info("Task cancelada: %s", task_name)
        return

    try:
        task.result()
    except Exception:
        logger.exception("Task encerrada com erro: %s", task_name)
    else:
        logger.warning("Task encerrada inesperadamente: %s", task_name)

def create_stream_task(coroutine, task_name: str) -> asyncio.Task:
    task = asyncio.create_task(coroutine, name=task_name)
    task.add_done_callback(
        lambda completed_task: log_task_result(completed_task, task_name)
    )
    return task

async def cancel_tasks(*tasks: asyncio.Task | None) -> None:
    active_tasks = [task for task in tasks if task is not None]

    for task in active_tasks:
        task.cancel()

    if active_tasks:
        await asyncio.gather(*active_tasks, return_exceptions=True)

if __name__ == "__main__":
    main()