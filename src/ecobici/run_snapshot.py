"""Punto de entrada: una corrida = un snapshot.

Local:      python -m ecobici.run_snapshot            (una vez)
            python -m ecobici.run_snapshot --loop 120 (cada 2 minutos)
En AWS:     el mismo código dentro de una Lambda disparada por EventBridge.
"""
from __future__ import annotations

import argparse
import json
import time
from datetime import datetime, timezone

from . import gbfs, quality, storage, weather


def run_once(with_weather: bool = True) -> dict:
    ts = datetime.now(timezone.utc)
    info_raw, status_raw = gbfs.fetch_feeds()
    storage.write_bronze("station_status", status_raw, ts)

    info = gbfs.station_information_df(info_raw)
    status = quality.check_status(gbfs.station_status_df(status_raw, ts), info)
    storage.write_silver("fact_station_status", status, ts)

    # El catálogo casi no cambia: se usa en cada corrida para validar,
    # pero solo se guarda una vez al día (medido: guardarlo siempre duplicaba el peso total).
    if not storage.has_silver_for_day("dim_station", ts):
        storage.write_bronze("station_information", info_raw, ts)
        storage.write_silver("dim_station", info, ts)

    result = {"ts": ts.isoformat(), "estaciones": len(info), "calidad": quality.summary(status)}
    if with_weather:
        w_raw = weather.fetch_weather()
        storage.write_bronze("weather", w_raw, ts)
        storage.write_silver("fact_weather_hourly", weather.weather_df(w_raw), ts)
    return result


def main() -> None:
    p = argparse.ArgumentParser()
    p.add_argument("--loop", type=int, default=0, help="segundos entre snapshots (0 = una sola vez)")
    args = p.parse_args()
    n = 0
    while True:
        # El clima cambia por hora: lo pedimos 1 de cada 30 corridas para no gastar llamadas.
        print(json.dumps(run_once(with_weather=(n % 30 == 0)), ensure_ascii=False))
        n += 1
        if not args.loop:
            break
        time.sleep(args.loop)


if __name__ == "__main__":
    main()
