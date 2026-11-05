from __future__ import annotations

import logging
from datetime import date, datetime, timedelta
from zoneinfo import ZoneInfo

from api.domain.open_meteo_client import OpenMeteoClient, OpenMeteoForecast
from api.domain.ocblacktop_client import OcblacktopClient
from api.schemas.formula1_events import Race, ScheduleItem
from api.schemas.weather import ForecastDay, RaceForecast, WeatherSnapshot

logger = logging.getLogger(__name__)


def _round(value: float, digits: int = 1) -> float:
    return round(value, digits)


def _require(values: list[float | None]) -> list[float]:
    return [value for value in values if value is not None]


def _daily_snapshot(forecast: OpenMeteoForecast, day: date) -> WeatherSnapshot | None:
    day_key = day.isoformat()
    try:
        index = forecast.daily.time.index(day_key)
    except ValueError:
        return None

    daily = forecast.daily
    t_max = daily.temperature_2m_max[index]
    t_min = daily.temperature_2m_min[index]
    weather_code = daily.weather_code[index]
    if t_max is None or t_min is None or weather_code is None:
        return None

    return WeatherSnapshot(
        t_max=_round(t_max),
        t_min=_round(t_min),
        precip_mm=_round(daily.precipitation_sum[index] or 0),
        precip_prob=_round(daily.precipitation_probability_max[index] or 0, 0),
        weather_code=weather_code,
        wind_speed=_round(daily.wind_speed_10m_max[index] or 0),
        wind_gust=_round(daily.wind_gusts_10m_max[index] or 0),
        wind_dir=_round(daily.wind_direction_10m_dominant[index] or 0, 0),
    )


def _hour_keys(
    start: datetime,
    end: datetime,
    timezone: ZoneInfo,
) -> list[str]:
    local_start = start.astimezone(timezone)
    local_end = end.astimezone(timezone)
    current = local_start.replace(minute=0, second=0, microsecond=0)
    end_hour = local_end.replace(minute=0, second=0, microsecond=0)
    keys: list[str] = []
    while current <= end_hour:
        keys.append(current.strftime("%Y-%m-%dT%H:00"))
        current += timedelta(hours=1)
    return keys


def _session_snapshot(
    forecast: OpenMeteoForecast,
    start: datetime,
    end: datetime,
    timezone: ZoneInfo,
) -> WeatherSnapshot | None:
    hourly = forecast.hourly
    time_index = {time_key: index for index, time_key in enumerate(hourly.time)}
    keys = _hour_keys(start, end, timezone)
    if not keys:
        return None

    indices: list[int] = []
    for key in keys:
        index = time_index.get(key)
        if index is not None:
            indices.append(index)

    if not indices:
        return None

    temps = _require([hourly.temperature_2m[index] for index in indices])
    if not temps:
        return None

    precip_values = [hourly.precipitation[index] or 0 for index in indices]
    prob_values = [hourly.precipitation_probability[index] or 0 for index in indices]
    wind_values = _require([hourly.wind_speed_10m[index] for index in indices])
    gust_values = [
        hourly.wind_gusts_10m[index] or 0 for index in indices
    ]

    start_index = time_index.get(keys[0])
    weather_code = (
        hourly.weather_code[start_index]
        if start_index is not None
        else hourly.weather_code[indices[0]]
    )
    if weather_code is None:
        return None

    max_gust = max(gust_values) if gust_values else 0
    gust_hour_index = next(
        index
        for index in indices
        if (hourly.wind_gusts_10m[index] or 0) == max_gust
    )
    wind_dir = hourly.wind_direction_10m[gust_hour_index] or 0

    return WeatherSnapshot(
        t_max=_round(max(temps)),
        t_min=_round(min(temps)),
        precip_mm=_round(sum(precip_values)),
        precip_prob=_round(max(prob_values), 0),
        weather_code=weather_code,
        wind_speed=_round(max(wind_values) if wind_values else 0),
        wind_gust=_round(max_gust),
        wind_dir=_round(wind_dir, 0),
    )


def _iter_event_dates(start: date, end: date) -> list[date]:
    days: list[date] = []
    current = start
    while current <= end:
        days.append(current)
        current += timedelta(days=1)
    return days


class WeatherForecastService:
    def __init__(
        self,
        ocblacktop_client: OcblacktopClient,
        open_meteo_client: OpenMeteoClient | None = None,
    ):
        self.ocblacktop_client = ocblacktop_client
        self.open_meteo_client = open_meteo_client or OpenMeteoClient()

    async def enrich_race(self, race: Race) -> Race:
        try:
            location = await self.ocblacktop_client.get_f1_location(str(race.location.id))
        except Exception as exc:
            logger.warning(
                "Failed to load location %s for race %s: %s",
                race.location.id,
                race.id,
                exc,
            )
            return race

        try:
            forecast = await self.open_meteo_client.get_forecast(
                latitude=float(location.latitude),
                longitude=float(location.longitude),
                timezone=location.timezone,
            )
        except Exception as exc:
            logger.warning("Failed to load forecast for race %s: %s", race.id, exc)
            return race

        if forecast is None:
            return race

        timezone = ZoneInfo(location.timezone)
        forecast_days = [
            ForecastDay(date=day, weather=weather)
            for day in _iter_event_dates(race.dateStart, race.dateEnd)
            if (weather := _daily_snapshot(forecast, day)) is not None
        ]

        schedule: list[ScheduleItem] = []
        for session in race.schedule:
            if session.startTime is None or session.endTime is None:
                schedule.append(session)
                continue

            weather = _session_snapshot(
                forecast,
                session.startTime,
                session.endTime,
                timezone,
            )
            schedule.append(session.model_copy(update={"weather": weather}))

        return race.model_copy(
            update={
                "forecast": RaceForecast(days=forecast_days) if forecast_days else None,
                "schedule": schedule,
            }
        )
