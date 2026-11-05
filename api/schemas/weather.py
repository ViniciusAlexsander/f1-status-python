from datetime import date

from pydantic import BaseModel


class WeatherSnapshot(BaseModel):
    t_max: float
    t_min: float
    precip_mm: float
    precip_prob: float
    weather_code: int
    wind_speed: float
    wind_gust: float
    wind_dir: float


class ForecastDay(BaseModel):
    date: date
    weather: WeatherSnapshot


class RaceForecast(BaseModel):
    days: list[ForecastDay]
