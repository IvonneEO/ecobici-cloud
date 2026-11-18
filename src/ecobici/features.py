"""Features para pronosticar demanda por estación y hora.

Regla de oro: ninguna feature puede usar información posterior al momento de la predicción.
- Rezagos (lag_1h, lag_24h, lag_168h) y promedio de las 3 horas previas: solo pasado.
- Perfil histórico por estación: se calcula SOLO con el periodo de entrenamiento.
- Clima de la hora pronosticada: en producción viene del pronóstico de Open-Meteo;
  en el histórico se usa lo observado (supuesto declarado: pronóstico ≈ observado a 1 h).
"""
from __future__ import annotations

import pandas as pd

# Días de descanso obligatorio (Ley Federal del Trabajo, art. 74) en el periodo de datos.
FESTIVOS = pd.to_datetime([
    "2026-01-01", "2026-02-02", "2026-03-16", "2026-05-01",
    "2026-09-16", "2026-11-16", "2026-12-25",
]).date

PEAK_HOURS = [7, 8, 9, 17, 18, 19]


def add_calendar(df: pd.DataFrame, ts_col: str = "ts_hour") -> pd.DataFrame:
    ts = df[ts_col]
    df["hora"] = ts.dt.hour.astype("int8")
    df["dia_semana"] = ts.dt.dayofweek.astype("int8")          # 0 = lunes
    df["fin_de_semana"] = df["dia_semana"] >= 5
    df["festivo"] = ts.dt.date.isin(FESTIVOS)
    df["dia_laboral"] = ~df["fin_de_semana"] & ~df["festivo"]
    dia = ts.dt.day
    df["quincena"] = (dia == 15) | (dia == ts.dt.days_in_month)
    df["hora_pico"] = df["dia_laboral"] & df["hora"].isin(PEAK_HOURS)
    return df


def add_lags(df: pd.DataFrame, cols=("retiros", "arribos"), lags=(1, 24, 168)) -> pd.DataFrame:
    """Rezagos por estación sobre la malla horaria completa (24 h por día)."""
    df = df.sort_values(["station", "ts_hour"]).reset_index(drop=True)
    g = df.groupby("station", sort=False)
    for c in cols:
        for k in lags:
            df[f"{c}_lag_{k}h"] = g[c].shift(k)
        df[f"{c}_media_3h"] = g[c].transform(lambda s: s.shift(1).rolling(3, min_periods=1).mean())
    return df


def station_profile(train: pd.DataFrame, cols=("retiros", "arribos")) -> pd.DataFrame:
    """Promedio por estación × tipo de día × hora, calculado solo con entrenamiento."""
    t = train[train["en_horario"]]
    prof = t.groupby(["station", "dia_laboral", "hora"])[list(cols)].mean()
    prof.columns = [f"{c}_perfil" for c in prof.columns]
    return prof.reset_index()


def add_profile(df: pd.DataFrame, profile: pd.DataFrame) -> pd.DataFrame:
    return df.merge(profile, on=["station", "dia_laboral", "hora"], how="left")


def add_weather(df: pd.DataFrame, weather: pd.DataFrame | None) -> pd.DataFrame:
    """weather: columnas ts_hour (hora local, sin zona), temperature_2m, precipitation."""
    if weather is None or weather.empty:
        df["temperature_2m"] = float("nan")
        df["precipitation"] = float("nan")
    else:
        w = weather[["ts_hour", "temperature_2m", "precipitation"]]
        df = df.merge(w, on="ts_hour", how="left")
    df["llueve"] = df["precipitation"] > 0.5
    return df
