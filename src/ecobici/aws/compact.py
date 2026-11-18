"""Compactación diaria: bronze (JSON por snapshot) -> silver (1 Parquet por día) + archivo del crudo.

La ejecuta el Job de Glue `curate` (glue/curate_job.py) a las 00:15 UTC.

Por qué existe:
- Resuelve el problema de "archivos pequeños" medido en la primera captura
  (un Parquet por snapshot pesaba más que el JSON crudo).
- Aplica las reglas de calidad y la bandera en_horario a todo el día.
- Es idempotente: correrla otra vez para el mismo día reescribe los mismos archivos.
- Archiva el crudo del día en un solo .jsonl.gz (archive/), porque S3 no transiciona
  a Glacier objetos de menos de 128 KB y cada snapshot pesa ~7 KB. Así los JSON
  individuales de bronze pueden expirar y el crudo se conserva barato.
"""
from __future__ import annotations

import gzip
import json
import logging
from concurrent.futures import ThreadPoolExecutor
from datetime import date, datetime, time, timedelta, timezone

import pandas as pd

from .. import gbfs, quality, storage, weather

log = logging.getLogger()
log.setLevel(logging.INFO)

CATALOG_LOOKBACK_DAYS = 7
READ_WORKERS = 16


def _epoch(key: str) -> datetime:
    return datetime.fromtimestamp(int(key.rsplit("/", 1)[-1].split(".")[0]), tz=timezone.utc)


def _read_all(keys: list[str]) -> list[tuple[datetime, dict]]:
    with ThreadPoolExecutor(max_workers=READ_WORKERS) as ex:
        payloads = list(ex.map(storage.read_bronze, keys))
    return [(_epoch(k), p) for k, p in zip(keys, payloads)]


def _archive(source: str, day: date, items: list[tuple[datetime, dict]]) -> dict:
    """Un .jsonl.gz por fuente y día con todos los payloads crudos, en orden."""
    lines = (json.dumps({"ingested_at": ts.isoformat(), "payload": p}, ensure_ascii=False) for ts, p in items)
    data = gzip.compress("\n".join(lines).encode("utf-8"))
    key = f"archive/{source}/dt={day:%Y-%m-%d}/snapshots.jsonl.gz"
    storage.put_bytes(key, data, "application/gzip")
    return {"archivo": key, "registros": len(items), "bytes": len(data)}


def _latest_catalog(day: date) -> tuple[dict, date]:
    """Catálogo del día; si no hay (p. ej. la ingesta empezó tarde), el más reciente previo."""
    for back in range(CATALOG_LOOKBACK_DAYS + 1):
        d = day - timedelta(days=back)
        keys = storage.list_keys(f"bronze/station_information/dt={d:%Y-%m-%d}/")
        if keys:
            return storage.read_bronze(keys[-1]), d
    raise RuntimeError(f"No hay catálogo de estaciones en los {CATALOG_LOOKBACK_DAYS} días previos a {day}")


def compact_status(day: date, ts_day: datetime) -> dict:
    keys = storage.list_keys(f"bronze/station_status/dt={day:%Y-%m-%d}/")
    if not keys:
        raise RuntimeError(f"No hay snapshots de station_status para {day}")

    cat_raw, cat_day = _latest_catalog(day)
    info = gbfs.station_information_df(cat_raw)

    snapshots = _read_all(keys)
    archivo = _archive("station_status", day, snapshots)

    # Las reglas se aplican snapshot por snapshot (dq_duplicado es dentro de un snapshot).
    frames = [quality.check_status(gbfs.station_status_df(p, ts), info) for ts, p in snapshots]
    df = pd.concat(frames, ignore_index=True)

    storage.write_silver("fact_station_status", df, ts_day, name="part-00000")
    storage.write_silver("dim_station", info, ts_day, name="part-00000")

    snaps = df["ingested_at"].drop_duplicates().sort_values()
    gaps = snaps.diff().dt.total_seconds().div(60)
    return {
        "snapshots": int(len(snaps)),
        "completitud_pct": round(len(snaps) / 720 * 100, 1),
        "huecos_mayores_5min": int((gaps > 5).sum()),
        "filas": int(len(df)),
        "catalogo_de": f"{cat_day}",
        "calidad": quality.summary(df),
        "archivo_crudo": archivo,
    }


def compact_weather(day: date, ts_day: datetime) -> dict:
    keys = storage.list_keys(f"bronze/weather/dt={day:%Y-%m-%d}/")
    if not keys:
        return {"consultas": 0}
    items = _read_all(keys)
    _archive("weather", day, items)
    frames = []
    for fetched_at, payload in items:
        w = weather.weather_df(payload)
        w["fetched_at"] = pd.Timestamp(fetched_at)
        frames.append(w)
    w = pd.concat(frames, ignore_index=True)

    # Cada consulta trae horas pasadas y pronóstico. Nos quedamos con lo ya observado
    # (hora <= momento de la consulta), del día compactado, y la consulta más reciente.
    start = pd.Timestamp(datetime.combine(day, time.min, tzinfo=timezone.utc))
    w = w[(w["ts_hour"] <= w["fetched_at"]) & (w["ts_hour"] >= start) & (w["ts_hour"] < start + pd.Timedelta(days=1))]
    w = w.sort_values("fetched_at").drop_duplicates("ts_hour", keep="last").sort_values("ts_hour")
    w = w.drop(columns="fetched_at").reset_index(drop=True)

    storage.write_silver("fact_weather_hourly", w, ts_day, name="part-00000")
    return {"consultas": len(keys), "horas": int(len(w))}


def compact_day(day: date) -> dict:
    ts_day = datetime.combine(day, time(12), tzinfo=timezone.utc)  # solo define la partición dt=
    return {
        "date": f"{day}",
        "station_status": compact_status(day, ts_day),
        "weather": compact_weather(day, ts_day),
    }


def handler(event, context=None) -> dict:
    raw = (event or {}).get("date")
    day = date.fromisoformat(raw) if raw else datetime.now(timezone.utc).date() - timedelta(days=1)
    result = compact_day(day)
    log.info(json.dumps(result, ensure_ascii=False, default=str))
    return result
