from datetime import date, datetime

from api.domain.open_meteo_client import OpenMeteoDaily, OpenMeteoForecast, OpenMeteoHourly
from api.services.weather_forecast_service import (
    _daily_snapshot,
    _session_snapshot,
)
from zoneinfo import ZoneInfo


def test_daily_snapshot_maps_open_meteo_daily_fields():
    forecast = OpenMeteoForecast(
        hourly=OpenMeteoHourly(
            time=[],
            temperature_2m=[],
            precipitation_probability=[],
            precipitation=[],
            wind_speed_10m=[],
            wind_gusts_10m=[],
            wind_direction_10m=[],
            weather_code=[],
        ),
        daily=OpenMeteoDaily(
            time=["2026-12-04"],
            temperature_2m_max=[22.0],
            temperature_2m_min=[18.0],
            precipitation_probability_max=[54.0],
            precipitation_sum=[2.6],
            weather_code=[51],
            wind_speed_10m_max=[18.0],
            wind_gusts_10m_max=[32.0],
            wind_direction_10m_dominant=[225.0],
        ),
    )

    snapshot = _daily_snapshot(forecast, date(2026, 12, 4))

    assert snapshot is not None
    assert snapshot.t_max == 22.0
    assert snapshot.t_min == 18.0
    assert snapshot.precip_mm == 2.6
    assert snapshot.precip_prob == 54.0
    assert snapshot.weather_code == 51
    assert snapshot.wind_speed == 18.0
    assert snapshot.wind_gust == 32.0
    assert snapshot.wind_dir == 225.0


def test_session_snapshot_aggregates_hourly_window():
    forecast = OpenMeteoForecast(
        hourly=OpenMeteoHourly(
            time=[
                "2026-12-06T13:00",
                "2026-12-06T14:00",
                "2026-12-06T15:00",
            ],
            temperature_2m=[26.0, 28.0, 27.0],
            precipitation_probability=[10.0, 40.0, 20.0],
            precipitation=[0.0, 1.2, 0.4],
            wind_speed_10m=[12.0, 18.0, 15.0],
            wind_gusts_10m=[20.0, 32.0, 24.0],
            wind_direction_10m=[180.0, 225.0, 200.0],
            weather_code=[1, 51, 3],
        ),
        daily=OpenMeteoDaily(
            time=[],
            temperature_2m_max=[],
            temperature_2m_min=[],
            precipitation_probability_max=[],
            precipitation_sum=[],
            weather_code=[],
            wind_speed_10m_max=[],
            wind_gusts_10m_max=[],
            wind_direction_10m_dominant=[],
        ),
    )

    tz = ZoneInfo("Asia/Dubai")
    start = datetime(2026, 12, 6, 13, 30, tzinfo=tz)
    end = datetime(2026, 12, 6, 15, 15, tzinfo=tz)

    snapshot = _session_snapshot(forecast, start, end, tz)

    assert snapshot is not None
    assert snapshot.t_max == 28.0
    assert snapshot.t_min == 26.0
    assert snapshot.precip_mm == 1.6
    assert snapshot.precip_prob == 40.0
    assert snapshot.weather_code == 1
    assert snapshot.wind_speed == 18.0
    assert snapshot.wind_gust == 32.0
    assert snapshot.wind_dir == 225.0
