"""Lambda de ingesta: guarda el JSON crudo en S3 bronze.

Disparada por EventBridge Scheduler:
  {"task": "status"}   cada 2 minutos  -> estado de estaciones (+ catálogo 1 vez al día)
  {"task": "weather"}  cada hora       -> clima de Open-Meteo

No usa pandas a propósito: así el paquete pesa unos KB, arranca rápido y cuesta menos.
La limpieza y las reglas de calidad se aplican una vez al día en la compactación.
Si algo falla, la función lanza la excepción: Lambda la registra como error,
reintenta una vez y la alarma de CloudWatch avisa por correo.
"""
from __future__ import annotations

import json
import logging
from datetime import datetime, timezone

from .. import feeds, storage

log = logging.getLogger()
log.setLevel(logging.INFO)


def run_status(ts: datetime) -> dict:
    info_raw, status_raw = feeds.fetch_feeds()
    n_status = len(status_raw.get("data", {}).get("stations", []))
    if n_status == 0:
        raise RuntimeError("station_status llegó sin estaciones")
    storage.write_bronze("station_status", status_raw, ts)

    catalogo_guardado = False
    if not storage.has_bronze_for_day("station_information", ts):
        storage.write_bronze("station_information", info_raw, ts)
        catalogo_guardado = True

    return {
        "task": "status",
        "ts": ts.isoformat(),
        "estaciones_status": n_status,
        "estaciones_catalogo": len(info_raw.get("data", {}).get("stations", [])),
        "catalogo_guardado": catalogo_guardado,
    }


def run_weather(ts: datetime) -> dict:
    raw = feeds.fetch_weather()
    horas = len(raw.get("hourly", {}).get("time", []))
    if horas == 0:
        raise RuntimeError("Open-Meteo respondió sin datos horarios")
    storage.write_bronze("weather", raw, ts)
    return {"task": "weather", "ts": ts.isoformat(), "horas": horas}


TASKS = {"status": run_status, "weather": run_weather}


def handler(event, context=None) -> dict:
    task = (event or {}).get("task", "status")
    if task not in TASKS:
        raise ValueError(f"Tarea desconocida: {task}")
    result = TASKS[task](datetime.now(timezone.utc))
    log.info(json.dumps(result, ensure_ascii=False))  # queda en CloudWatch Logs
    return result
