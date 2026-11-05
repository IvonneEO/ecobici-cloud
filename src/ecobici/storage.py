"""Escritura local con el mismo particionado que usaremos en S3.

bronze/<fuente>/dt=YYYY-MM-DD/hour=HH/<epoch>.json.gz
silver/<tabla>/dt=YYYY-MM-DD/<epoch>.parquet

El particionado por fecha (dt=) es lo que permite que Athena lea solo
los días que se consultan, y por lo tanto cobre menos.
Cambiar a S3 es reemplazar este módulo por boto3 put_object.
"""
from __future__ import annotations

import gzip
import json
from datetime import datetime
from pathlib import Path

import pandas as pd

from . import config


def _partition(ts: datetime) -> str:
    return f"dt={ts:%Y-%m-%d}"


def write_bronze(source: str, payload: dict, ts: datetime) -> Path:
    path = config.DATA_DIR / "bronze" / source / _partition(ts) / f"hour={ts:%H}" / f"{int(ts.timestamp())}.json.gz"
    path.parent.mkdir(parents=True, exist_ok=True)
    with gzip.open(path, "wt", encoding="utf-8") as f:
        json.dump(payload, f, ensure_ascii=False)
    return path


def has_silver_for_day(table: str, ts: datetime) -> bool:
    """True si ya existe al menos un archivo de la tabla para la fecha (UTC) de ts."""
    folder = config.DATA_DIR / "silver" / table / _partition(ts)
    return folder.exists() and any(folder.glob("*.parquet"))


def write_silver(table: str, df: pd.DataFrame, ts: datetime) -> Path:
    path = config.DATA_DIR / "silver" / table / _partition(ts) / f"{int(ts.timestamp())}.parquet"
    path.parent.mkdir(parents=True, exist_ok=True)
    df.to_parquet(path, index=False, compression="snappy")
    return path
