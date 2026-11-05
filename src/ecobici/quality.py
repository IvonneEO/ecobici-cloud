"""Reglas de calidad de datos para cada snapshot de station_status.

No borramos filas: las marcamos. Así el modelo decide qué excluir y
podemos medir la calidad del feed a lo largo del tiempo.
"""
from __future__ import annotations

import pandas as pd

STALE_MINUTES = 30  # una estación que no reporta en 30 min probablemente está caída


def check_status(status: pd.DataFrame, info: pd.DataFrame) -> pd.DataFrame:
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
    return df.drop(columns="capacity")


def summary(df: pd.DataFrame) -> dict[str, int]:
    return {c: int(df[c].sum()) for c in df.columns if c.startswith("dq_")} | {"filas": len(df)}
