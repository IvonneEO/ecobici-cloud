"""Clima horario de CDMX desde Open-Meteo (atribución: Weather data by Open-Meteo.com)."""
from __future__ import annotations

import pandas as pd

from . import config
from .http import get_json

HOURLY_VARS = ["temperature_2m", "precipitation", "rain", "wind_speed_10m", "weather_code"]


def fetch_weather(past_days: int = 1, forecast_days: int = 1) -> dict:
    return get_json(
        config.OPEN_METEO_URL,
        params={
            "latitude": config.CDMX_LAT,
            "longitude": config.CDMX_LON,
            "hourly": ",".join(HOURLY_VARS),
            "timezone": "UTC",
            "past_days": past_days,
            "forecast_days": forecast_days,
        },
    )


def weather_df(payload: dict) -> pd.DataFrame:
    h = payload["hourly"]
    df = pd.DataFrame({"ts_hour": pd.to_datetime(h["time"], utc=True)})
    for v in HOURLY_VARS:
        df[v] = pd.to_numeric(pd.Series(h.get(v, [None] * len(df))), errors="coerce")
    return df
