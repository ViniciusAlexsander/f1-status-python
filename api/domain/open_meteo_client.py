import logging
from typing import Any

import httpx
from pydantic import BaseModel, ValidationError

logger = logging.getLogger(__name__)

OPEN_METEO_FORECAST_URL = "https://api.open-meteo.com/v1/forecast"

HOURLY_PARAMS = (
    "temperature_2m,precipitation_probability,precipitation,"
    "wind_speed_10m,wind_gusts_10m,wind_direction_10m,weather_code"
)
DAILY_PARAMS = (
    "temperature_2m_max,temperature_2m_min,precipitation_probability_max,"
    "precipitation_sum,weather_code,wind_speed_10m_max,wind_gusts_10m_max,"
    "wind_direction_10m_dominant,sunrise,sunset"
)


class OpenMeteoHourly(BaseModel):
    time: list[str]
    temperature_2m: list[float | None]
    precipitation_probability: list[float | None]
    precipitation: list[float | None]
    wind_speed_10m: list[float | None]
    wind_gusts_10m: list[float | None]
    wind_direction_10m: list[float | None]
    weather_code: list[int | None]


class OpenMeteoDaily(BaseModel):
    time: list[str]
    temperature_2m_max: list[float | None]
    temperature_2m_min: list[float | None]
    precipitation_probability_max: list[float | None]
    precipitation_sum: list[float | None]
    weather_code: list[int | None]
    wind_speed_10m_max: list[float | None]
    wind_gusts_10m_max: list[float | None]
    wind_direction_10m_dominant: list[float | None]


class OpenMeteoForecast(BaseModel):
    hourly: OpenMeteoHourly
    daily: OpenMeteoDaily


class OpenMeteoClient:
    def __init__(self, timeout: float = 15.0):
        self.timeout = timeout

    async def get_forecast(
        self,
        latitude: float,
        longitude: float,
        timezone: str,
        forecast_days: int = 16,
    ) -> OpenMeteoForecast | None:
        params: dict[str, Any] = {
            "latitude": latitude,
            "longitude": longitude,
            "hourly": HOURLY_PARAMS,
            "daily": DAILY_PARAMS,
            "forecast_days": forecast_days,
            "timezone": timezone,
            "wind_speed_unit": "kmh",
        }

        try:
            async with httpx.AsyncClient(timeout=self.timeout) as client:
                response = await client.get(
                    OPEN_METEO_FORECAST_URL,
                    params=params,
                    headers={"Accept": "application/json"},
                )
                response.raise_for_status()
                payload = response.json()
        except (httpx.HTTPError, ValidationError) as exc:
            logger.warning("Open-Meteo forecast request failed: %s", exc)
            return None

        try:
            return OpenMeteoForecast.model_validate(payload)
        except ValidationError as exc:
            logger.warning("Open-Meteo returned an invalid response: %s", exc)
            return None
