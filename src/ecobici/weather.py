"""Clima horario de CDMX desde Open-Meteo (atribución: Weather data by Open-Meteo.com)."""
from __future__ import annotations

HOURLY_VARS = ["temperature_2m", "precipitation", "rain", "wind_speed_10m", "weather_code"]


def fetch_weather(past_days: int = 1, forecast_days: int = 1) -> dict:
    from .feeds import fetch_weather as _fetch

    return _fetch(past_days, forecast_days)


def weather_df(payload: dict):
    import pandas as pd

    h = payload["hourly"]
    df = pd.DataFrame({"ts_hour": pd.to_datetime(h["time"], utc=True)})
    for v in HOURLY_VARS:
        # Siempre float64: así el tipo en Parquet (y en Athena) no cambia si llega un nulo.
        df[v] = pd.to_numeric(pd.Series(h.get(v, [None] * len(df))), errors="coerce").astype("float64")
    return df
