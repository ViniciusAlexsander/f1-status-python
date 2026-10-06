import asyncio
import logging
from datetime import datetime, timedelta, timezone
from typing import Any
from zoneinfo import ZoneInfo

from redis.asyncio import Redis

from api.core.config import get_settings
from api.dependencies import create_livetiming_signalrcore_client
from api.domain.ocblacktop_client import OcblacktopClient
from api.schemas.formula1_events import Race
from api.services.live_session_service import LiveSessionService
from api.services.live_timing_cache import LiveTimingCache
from api.services.race_service import RaceService
from api.services.timing_service import TimingService
from api.services.worker_control_service import WorkerControlService, WorkerSchedule

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)


async def run_worker() -> None:
    logger.info("Worker iniciado")
    settings = get_settings()
    worker_timezone = ZoneInfo(settings.worker_timezone)

    race_service = RaceService(
        client=OcblacktopClient(
            base_url=settings.ocblacktop_api_base_url,
            api_key=settings.ocblacktop_api_key,
        )
    )

    signalr_client = create_livetiming_signalrcore_client(settings)
    redis = Redis.from_url(settings.redis_url, decode_responses=True)
    cache = LiveTimingCache(redis)
    worker_control = WorkerControlService(
        redis=redis,
        schedule_ttl_seconds=settings.worker_schedule_cache_ttl_seconds,
    )
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
            now = datetime.now(timezone.utc)
            schedule = await worker_control.get_schedule()

            schedule_live_active = is_schedule_live_active(schedule, now)
            schedule_refresh_due = should_refresh_schedule(
                schedule,
                now,
                settings.worker_schedule_refresh_seconds,
            )
            should_refresh = (
                schedule is None
                or (schedule_refresh_due and not schedule_live_active)
            )

            if should_refresh:
                try:
                    schedule = await refresh_worker_schedule(
                        race_service,
                        worker_control,
                        now,
                        settings.worker_start_before_minutes,
                        settings.worker_stop_after_minutes,
                    )
                    schedule_live_active = is_schedule_live_active(schedule, now)
                except Exception:
                    logger.exception("Erro ao atualizar agenda do worker")

                    if schedule is None:
                        await cancel_tasks(live_timing_task, live_session_task)
                        live_timing_task = None
                        live_session_task = None
                        await asyncio.sleep(settings.worker_error_retry_seconds)
                        continue

            should_listen = schedule_live_active

            if should_listen:
                if live_timing_task is None or live_timing_task.done():
                    logger.info("Iniciando stream de live timing")
                    live_timing_task = create_stream_task(
                        start_save_live_timing(timing_service),
                        "live-timing",
                    )

                if live_session_task is None or live_session_task.done():
                    logger.info("Iniciando stream de live session")
                    live_session_task = create_stream_task(
                        start_save_live_session(session_service),
                        "live-session",
                    )

            else:
                logger.info(
                    "Nenhuma sessao em andamento. Encerrando streams de live timing"
                )

                await cancel_tasks(live_timing_task, live_session_task)
                live_timing_task = None
                live_session_task = None

            poll_seconds = seconds_until_next_worker_run(
                schedule,
                now,
                should_listen,
                settings.worker_idle_check_seconds,
                settings.worker_near_session_check_seconds,
            )

            if schedule is None:
                poll_seconds = min(
                    poll_seconds,
                    seconds_until_next_daily_run(now, worker_timezone),
                )

            await asyncio.sleep(poll_seconds)
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
    return any(
        scheduled_session.status == "ongoing"
        for scheduled_session in onGoingWeekend.schedule
    )


def seconds_until_next_daily_run(
    now: datetime,
    worker_timezone: ZoneInfo,
    run_hour: int = 1,
) -> int:
    local_now = now.astimezone(worker_timezone)
    next_run = local_now.replace(
        hour=run_hour,
        minute=0,
        second=0,
        microsecond=0,
    )
    if next_run <= local_now:
        next_run += timedelta(days=1)

    return max(1, int((next_run - local_now).total_seconds()))


def should_listen_to_live_streams(
    race: Race,
    now: datetime,
    start_before_minutes: int,
    stop_after_minutes: int = 240,
) -> bool:
    if have_ongoing_session(race):
        return True

    for scheduled_session in race.schedule:
        if (
            scheduled_session.status == "finished"
            or scheduled_session.startTime is None
        ):
            continue

        start_time = normalize_datetime(scheduled_session.startTime, now)
        listen_from = start_time - timedelta(minutes=start_before_minutes)
        listen_until = start_time + timedelta(minutes=stop_after_minutes)

        if listen_from <= now <= listen_until:
            return True

    return False


async def refresh_worker_schedule(
    race_service: RaceService,
    worker_control: WorkerControlService,
    now: datetime,
    start_before_minutes: int,
    stop_after_minutes: int,
) -> WorkerSchedule | None:
    races = await race_service.list_races()
    schedule = build_worker_schedule(
        races.data.races,
        now,
        start_before_minutes,
        stop_after_minutes,
    )

    if schedule is None:
        await worker_control.clear_schedule()
    else:
        await worker_control.save_schedule(schedule)

    return schedule


def build_worker_schedule(
    races: list[Any],
    now: datetime,
    start_before_minutes: int,
    stop_after_minutes: int,
) -> WorkerSchedule | None:
    ongoing = find_ongoing_session(races)

    if ongoing is not None:
        race, session = ongoing
        start_time = normalize_datetime(session.startTime, now)
        return WorkerSchedule(
            raceId=str(race.id),
            raceName=race.name,
            sessionId=str(session.id),
            sessionName=session.name,
            startTime=start_time,
            listenFrom=now,
            listenUntil=now + timedelta(minutes=stop_after_minutes),
            lastRefreshAt=now,
        )

    candidates = []

    for race in races:
        for session in race.schedule:
            if session.status == "finished" or session.startTime is None:
                continue

            start_time = normalize_datetime(session.startTime, now)
            listen_until = start_time + timedelta(minutes=stop_after_minutes)

            if listen_until < now:
                continue

            candidates.append((start_time, race, session))

    if not candidates:
        return None

    start_time, race, session = sorted(candidates, key=lambda item: item[0])[0]
    return WorkerSchedule(
        raceId=str(race.id),
        raceName=race.name,
        sessionId=str(session.id),
        sessionName=session.name,
        startTime=start_time,
        listenFrom=start_time - timedelta(minutes=start_before_minutes),
        listenUntil=start_time + timedelta(minutes=stop_after_minutes),
        lastRefreshAt=now,
    )


def find_ongoing_session(races: list[Any]) -> tuple[Any, Any] | None:
    for race in races:
        for session in race.schedule:
            if session.status == "ongoing":
                return race, session

    return None


def normalize_datetime(value: datetime | None, fallback: datetime) -> datetime:
    resolved = value or fallback

    if resolved.tzinfo is None:
        return resolved.replace(tzinfo=timezone.utc)

    return resolved.astimezone(timezone.utc)


def is_schedule_live_active(
    schedule: WorkerSchedule | None,
    now: datetime,
) -> bool:
    return schedule is not None and schedule.listenFrom <= now <= schedule.listenUntil


def should_refresh_schedule(
    schedule: WorkerSchedule | None,
    now: datetime,
    refresh_seconds: int,
) -> bool:
    if schedule is None:
        return True

    return schedule.lastRefreshAt + timedelta(seconds=refresh_seconds) <= now


def seconds_until_next_worker_run(
    schedule: WorkerSchedule | None,
    now: datetime,
    is_listening: bool,
    idle_check_seconds: int,
    near_session_check_seconds: int,
) -> int:
    if is_listening:
        candidates = [near_session_check_seconds]

        if schedule and schedule.listenUntil >= now:
            candidates.append(seconds_between(now, schedule.listenUntil))

        return max(1, min(candidates))

    if schedule and schedule.listenFrom > now:
        return max(
            1,
            min(seconds_between(now, schedule.listenFrom), idle_check_seconds),
        )

    return max(1, idle_check_seconds)


def seconds_between(start: datetime, end: datetime) -> int:
    return max(1, int((end - start).total_seconds()))


async def start_save_live_timing(timing_service: TimingService) -> None:
    try:
        async for event in timing_service.stream_timing_data():
            logger.info("Atualizacao de timing recebida: %s", event["receivedAt"])
    except asyncio.CancelledError:
        logger.info("Stream de live timing cancelado")
        raise


async def start_save_live_session(
    session_service: LiveSessionService,
) -> None:
    try:
        async for event in session_service.stream_session_state():
            logger.info("Estado da sessao recebido: %s", event)
    except asyncio.CancelledError:
        logger.info("Stream da sessao cancelado")
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
