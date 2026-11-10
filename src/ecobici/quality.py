"""Reglas de calidad de datos para cada snapshot de station_status.

No borramos filas: las marcamos. Así el modelo decide qué excluir y
podemos medir la calidad del feed a lo largo del tiempo.
"""
from __future__ import annotations

import pandas as pd

from . import config

STALE_MINUTES = 30  # una estación que no reporta en 30 min probablemente está caída


def en_horario(ts_utc: pd.Series) -> pd.Series:
    """True si la hora local de CDMX está dentro del horario de operación de Ecobici.

    Medido en la primera captura: de 1:00 a 4:59 el sistema está cerrado y las
    reglas marcan prácticamente todas las estaciones, así que esas horas no se
    evalúan ni se usan para el modelo.
    """
    hora = ts_utc.dt.tz_convert(config.TIMEZONE).dt.hour
    return ~hora.between(config.CLOSED_FROM_HOUR, config.CLOSED_UNTIL_HOUR - 1)


def check_status(status: pd.DataFrame, info: pd.DataFrame) -> pd.DataFrame:
    """Marca un snapshot. Debe aplicarse por snapshot, no a varios juntos
    (dq_duplicado busca repetidos dentro del mismo snapshot)."""
    df = status.merge(info[["station_id", "capacity"]], on="station_id", how="left")
    total = (
        df["num_bikes_available"].fillna(0)
        + df["num_bikes_disabled"].fillna(0)
        + df["num_docks_available"].fillna(0)
        + df["num_docks_disabled"].fillna(0)
    )
    age_min = (df["ingested_at"] - df["last_reported_ts"]).dt.total_seconds() / 60

    df["dq_sin_catalogo"] = df["capacity"].isna()
    df["dq_negativo"] = (df[["num_bikes_available", "num_docks_available"]] < 0).any(axis=1)
    df["dq_excede_capacidad"] = df["capacity"].notna() & (total > df["capacity"])
    df["dq_reporte_viejo"] = age_min > STALE_MINUTES
    df["dq_fuera_servicio"] = ~df["is_installed"].fillna(False) | ~df["is_renting"].fillna(False)
    df["dq_duplicado"] = df.duplicated(subset=["station_id", "last_reported"], keep="first")

    flags = [c for c in df.columns if c.startswith("dq_")]
    df["dq_ok"] = ~df[flags].any(axis=1)
    df["en_horario"] = en_horario(df["ingested_at"])
    return df.drop(columns="capacity")


def summary(df: pd.DataFrame) -> dict:
    out: dict = {c: int(df[c].sum()) for c in df.columns if c.startswith("dq_")} | {"filas": len(df)}
    if "en_horario" in df.columns:
        op = df[df["en_horario"]]
        out["pct_ok_en_horario"] = round(float(op["dq_ok"].mean()) * 100, 1) if len(op) else None
    return out
