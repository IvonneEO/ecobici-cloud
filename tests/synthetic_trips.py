"""Generador de viajes sintéticos con el formato real del CSV de Ecobici.

Sirve para probar el pipeline de modelado sin los archivos reales (que traen datos
personales y no se suben al repo). Incluye patrones de hora pico, estaciones "origen"
(se vacían en la mañana) y "destino" (se llenan), lluvia que reduce la demanda y
algunos registros sucios a propósito.
"""
from __future__ import annotations

from pathlib import Path

import numpy as np
import pandas as pd


def hourly_shape(hour: np.ndarray, laboral: np.ndarray) -> np.ndarray:
    closed = (hour >= 1) & (hour <= 4)
    lab = 0.3 + 2.2 * np.exp(-((hour - 8) ** 2) / 1.5) + 1.8 * np.exp(-((hour - 18) ** 2) / 3) + 0.6 * ((hour >= 12) & (hour <= 16))
    fds = 0.3 + 1.2 * np.exp(-((hour - 12) ** 2) / 8)
    return np.where(closed, 0, np.where(laboral, lab, fds))


def make(n_stations=40, start="2026-09-01", end="2026-11-01", seed=0):
    rng = np.random.default_rng(seed)
    stations = np.array([f"{i:03d}" for i in range(1, n_stations + 1)])
    tipo = rng.choice(["origen", "destino", "mixta"], n_stations, p=[0.3, 0.3, 0.4])
    scale = rng.uniform(0.5, 2.0, n_stations)

    hours = pd.date_range(start, end, freq="h", inclusive="left")
    h = hours.hour.values
    laboral = (hours.dayofweek.values < 5) & (hours.normalize() != pd.Timestamp("2026-09-16"))
    rain = np.zeros(len(hours))
    tarde = (h >= 15) & (h <= 20)
    rain[tarde] = rng.choice([0, 0, 0, 1.5, 4.0], tarde.sum())
    weather = pd.DataFrame({"ts_hour": hours, "temperature_2m": 18 + 6 * np.sin((h - 9) / 24 * 2 * np.pi),
                            "precipitation": rain})

    rows = []
    for s, t, k in zip(stations, tipo, scale):
        lam = hourly_shape(h, laboral) * k
        if t == "origen":
            lam = lam * np.where((h >= 6) & (h <= 10) & laboral, 2.0, 0.7)
        elif t == "destino":
            lam = lam * np.where((h >= 6) & (h <= 10) & laboral, 0.4, 1.2)
        lam = lam * np.where(rain > 1, 0.5, 1.0)
        n = rng.poisson(lam)
        idx = np.repeat(np.arange(len(hours)), n)
        if not len(idx):
            continue
        ts_out = hours[idx] + pd.to_timedelta(rng.integers(0, 3600, len(idx)), unit="s")
        # Destino: en la mañana laboral, más probable una estación "destino".
        w = np.where(tipo == "destino", 3.0, 1.0)
        dest = rng.choice(stations, len(idx), p=w / w.sum())
        dur = pd.to_timedelta(rng.exponential(12, len(idx)) * 60 + 90, unit="s")
        rows.append(pd.DataFrame({"station_out": s, "station_in": dest, "ts_out": ts_out, "ts_in": ts_out + dur}))
    v = pd.concat(rows, ignore_index=True)

    csv = pd.DataFrame({
        "Genero_Usuario": rng.choice(["M", "F", "O", "?"], len(v), p=[0.68, 0.28, 0.02, 0.02]),
        "Edad_Usuario": rng.integers(17, 70, len(v)).astype(str),
        "Bici": rng.integers(1_000_000, 9_000_000, len(v)).astype(str),
        "Ciclo_Estacion_Retiro": v["station_out"],
        "Fecha_Retiro": v["ts_out"].dt.strftime("%d/%m/%Y"),
        "Hora_Retiro": v["ts_out"].dt.strftime("%H:%M:%S"),
        "Ciclo_EstacionArribo": v["station_in"],
        "Fecha_Arribo": v["ts_in"].dt.strftime("%d/%m/%Y"),
        "Hora_Arribo": v["ts_in"].dt.strftime("%H:%M:%S"),
    })
    # Registros sucios a propósito.
    csv.loc[0, "Hora_Retiro"] = "xx"
    csv.loc[1, "Fecha_Arribo"] = "01/08/2026"                     # llega antes de salir
    csv.loc[2, "Fecha_Arribo"] = "28/10/2026"                     # más de 24 h
    csv.loc[3, ["Hora_Arribo"]] = csv.loc[3, "Hora_Retiro"]       # 0 min
    return csv, weather, dict(zip(stations, tipo))


def write(folder: Path, **kw):
    folder.mkdir(parents=True, exist_ok=True)
    csv, weather, tipos = make(**kw)
    ts = pd.to_datetime(csv["Fecha_Retiro"], format="%d/%m/%Y", errors="coerce")
    for (y, m), part in csv.groupby([ts.dt.year, ts.dt.month]):
        part.to_csv(folder / f"public_data_web_{int(y)}-{int(m):02d}.csv", index=False)
    return weather, tipos
