"""Descarga de los feeds GBFS sin dependencias pesadas (sin pandas).

La usa la Lambda de ingesta, que solo guarda el JSON crudo en bronze.
"""
from __future__ import annotations

from . import config
from .http import get_json


def discover_feeds(discovery: dict) -> dict[str, str]:
    """Devuelve {nombre_feed: url}. GBFS agrupa feeds por idioma (es/en/...)."""
    data = discovery.get("data", {})
    for lang in ("es", "en"):
        if lang in data:
            feeds = data[lang]["feeds"]
            break
    else:
        feeds = next(iter(data.values()), {}).get("feeds", [])
    return {f["name"]: f["url"] for f in feeds}


def fetch_feeds() -> tuple[dict, dict]:
    """Devuelve (station_information, station_status) crudos."""
    urls = discover_feeds(get_json(config.GBFS_DISCOVERY_URL))
    missing = {"station_information", "station_status"} - urls.keys()
    if missing:
        raise RuntimeError(f"El feed de descubrimiento no trae: {sorted(missing)}")
    return get_json(urls["station_information"]), get_json(urls["station_status"])


def fetch_weather(past_days: int = 1, forecast_days: int = 1) -> dict:
    """Clima horario de CDMX desde Open-Meteo (atribución: Weather data by Open-Meteo.com)."""
    from .weather import HOURLY_VARS  # constante sin pandas

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
