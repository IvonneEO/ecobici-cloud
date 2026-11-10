"""Almacenamiento con el mismo particionado en local y en S3.

bronze/<fuente>/dt=YYYY-MM-DD/hour=HH/<epoch>.json.gz
silver/<tabla>/dt=YYYY-MM-DD/<archivo>.parquet

Las particiones (dt=) usan la fecha UTC. Permiten que Athena lea solo los días
que se consultan, y por lo tanto cobre menos.

Backend: ECOBICI_STORAGE=local (carpeta DATA_DIR) o s3 (bucket ECOBICI_BUCKET).
pandas y boto3 se importan solo cuando se necesitan, para que la Lambda de
ingesta no cargue librerías pesadas.
"""
from __future__ import annotations

import gzip
import io
import json
from datetime import datetime
from pathlib import Path

from . import config

_s3_client = None


def _s3():
    global _s3_client
    if _s3_client is None:
        import boto3

        _s3_client = boto3.client("s3")
    return _s3_client


def _is_s3() -> bool:
    return config.STORAGE == "s3"


def _uri(key: str) -> str:
    return f"s3://{config.BUCKET}/{key}" if _is_s3() else str(config.DATA_DIR / key)


# ---------- Operaciones básicas ----------

def put_bytes(key: str, data: bytes, content_type: str) -> str:
    if _is_s3():
        _s3().put_object(Bucket=config.BUCKET, Key=key, Body=data, ContentType=content_type)
    else:
        path = config.DATA_DIR / key
        path.parent.mkdir(parents=True, exist_ok=True)
        path.write_bytes(data)
    return _uri(key)


def get_bytes(key: str) -> bytes:
    if _is_s3():
        return _s3().get_object(Bucket=config.BUCKET, Key=key)["Body"].read()
    return (config.DATA_DIR / key).read_bytes()


def list_keys(prefix: str) -> list[str]:
    """Llaves (rutas relativas) que empiezan con prefix, ordenadas."""
    if _is_s3():
        keys: list[str] = []
        for page in _s3().get_paginator("list_objects_v2").paginate(Bucket=config.BUCKET, Prefix=prefix):
            keys += [o["Key"] for o in page.get("Contents", [])]
        return sorted(keys)
    base = config.DATA_DIR
    folder = base / prefix
    if not folder.exists():
        return []
    return sorted(str(p.relative_to(base)) for p in folder.rglob("*") if p.is_file())


def has_any(prefix: str) -> bool:
    if _is_s3():
        r = _s3().list_objects_v2(Bucket=config.BUCKET, Prefix=prefix, MaxKeys=1)
        return r.get("KeyCount", 0) > 0
    folder = config.DATA_DIR / prefix
    return folder.exists() and any(p.is_file() for p in folder.rglob("*"))


# ---------- Capas ----------

def partition(ts: datetime) -> str:
    return f"dt={ts:%Y-%m-%d}"


def bronze_key(source: str, ts: datetime) -> str:
    return f"bronze/{source}/{partition(ts)}/hour={ts:%H}/{int(ts.timestamp())}.json.gz"


def write_bronze(source: str, payload: dict, ts: datetime) -> str | Path:
    data = gzip.compress(json.dumps(payload, ensure_ascii=False).encode("utf-8"))
    uri = put_bytes(bronze_key(source, ts), data, "application/gzip")
    return uri if _is_s3() else Path(uri)


def read_bronze(key: str) -> dict:
    return json.loads(gzip.decompress(get_bytes(key)).decode("utf-8"))


def has_bronze_for_day(source: str, ts: datetime) -> bool:
    return has_any(f"bronze/{source}/{partition(ts)}/")


def has_silver_for_day(table: str, ts: datetime) -> bool:
    """True si ya existe al menos un archivo de la tabla para la fecha (UTC) de ts."""
    return has_any(f"silver/{table}/{partition(ts)}/")


def write_silver(table: str, df, ts: datetime, name: str | None = None) -> str | Path:
    buf = io.BytesIO()
    # Milisegundos: Athena no lee timestamps en nanosegundos (el default de pandas).
    df.to_parquet(buf, index=False, compression="snappy",
                  coerce_timestamps="ms", allow_truncated_timestamps=True)
    key = f"silver/{table}/{partition(ts)}/{name or int(ts.timestamp())}.parquet"
    uri = put_bytes(key, buf.getvalue(), "application/vnd.apache.parquet")
    return uri if _is_s3() else Path(uri)
