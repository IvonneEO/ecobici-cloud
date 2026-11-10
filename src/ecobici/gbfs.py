"""Transformación del feed GBFS de Ecobici a tablas tipadas (silver).

La descarga vive en feeds.py (sin pandas) para que la Lambda de ingesta sea ligera.
"""
from __future__ import annotations

from datetime import datetime, timezone

import pandas as pd

from .feeds import discover_feeds, fetch_feeds  # noqa: F401  (se reexportan)


# ---------- Transformación a silver ----------

INFO_COLS = {
    "station_id": "string",
    "name": "string",
    "short_name": "string",
    "lat": "float64",
    "lon": "float64",
    "capacity": "Int16",
}

STATUS_COLS = {
    "station_id": "string",
    "num_bikes_available": "Int16",
    "num_bikes_disabled": "Int16",
    "num_docks_available": "Int16",
    "num_docks_disabled": "Int16",
    "is_installed": "boolean",
    "is_renting": "boolean",
    "is_returning": "boolean",
    "last_reported": "Int64",  # epoch en segundos (UTC)
}


def _select(records: list[dict], cols: dict[str, str]) -> pd.DataFrame:
    df = pd.DataFrame(records)
    for c in cols:
        if c not in df.columns:  # tolera campos opcionales ausentes
            df[c] = pd.NA
    df = df[list(cols)]
    # GBFS 1.x manda booleanos como 0/1; los normalizamos.
    for c, t in cols.items():
        if t == "boolean":
            df[c] = df[c].map({1: True, 0: False, True: True, False: False})
    return df.astype(cols)


def station_information_df(payload: dict) -> pd.DataFrame:
    return _select(payload["data"]["stations"], INFO_COLS)


def station_status_df(payload: dict, ingested_at: datetime | None = None) -> pd.DataFrame:
    df = _select(payload["data"]["stations"], STATUS_COLS)
    df["last_reported_ts"] = pd.to_datetime(df["last_reported"], unit="s", utc=True)
    df["feed_last_updated_ts"] = pd.to_datetime(payload.get("last_updated"), unit="s", utc=True)
    df["ingested_at"] = pd.Timestamp(ingested_at or datetime.now(timezone.utc))
    return df
