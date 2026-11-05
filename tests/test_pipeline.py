"""Pruebas offline: no tocan la red. Usan payloads con la estructura GBFS real."""
from datetime import datetime, timezone

import pandas as pd

from ecobici import gbfs, quality, storage, weather

NOW = datetime(2026, 11, 3, 18, 0, tzinfo=timezone.utc)
T = int(NOW.timestamp())

DISCOVERY = {"data": {"es": {"feeds": [
    {"name": "station_information", "url": "https://x/si.json"},
    {"name": "station_status", "url": "https://x/ss.json"},
]}}}

INFO = {"last_updated": T, "ttl": 60, "data": {"stations": [
    {"station_id": "1", "name": "Reforma-Insurgentes", "short_name": "001", "lat": 19.43, "lon": -99.16, "capacity": 20},
    {"station_id": "2", "name": "Ámsterdam", "lat": 19.41, "lon": -99.17, "capacity": 10},  # sin short_name
]}}

STATUS = {"last_updated": T, "ttl": 60, "data": {"stations": [
    # sana
    {"station_id": "1", "num_bikes_available": 5, "num_bikes_disabled": 1, "num_docks_available": 14,
     "num_docks_disabled": 0, "is_installed": 1, "is_renting": 1, "is_returning": 1, "last_reported": T - 60},
    # excede capacidad (8 + 5 > 10) y reporte viejo (2 h)
    {"station_id": "2", "num_bikes_available": 8, "num_bikes_disabled": 0, "num_docks_available": 5,
     "num_docks_disabled": 0, "is_installed": True, "is_renting": True, "is_returning": True, "last_reported": T - 7200},
    # estación que no existe en el catálogo y está fuera de servicio
    {"station_id": "999", "num_bikes_available": 0, "num_docks_available": 0,
     "is_installed": 0, "is_renting": 0, "is_returning": 0, "last_reported": T},
]}}


def test_discover_feeds():
    assert gbfs.discover_feeds(DISCOVERY)["station_status"] == "https://x/ss.json"


def test_info_tolera_campos_ausentes():
    df = gbfs.station_information_df(INFO)
    assert len(df) == 2 and df.loc[1, "short_name"] is pd.NA
    assert str(df["capacity"].dtype) == "Int16"


def test_status_tipos_y_booleanos():
    df = gbfs.station_status_df(STATUS, NOW)
    assert df["is_renting"].tolist() == [True, True, False]
    assert df.loc[0, "last_reported_ts"] == pd.Timestamp(T - 60, unit="s", tz="UTC")


def test_reglas_de_calidad():
    info = gbfs.station_information_df(INFO)
    df = quality.check_status(gbfs.station_status_df(STATUS, NOW), info).set_index("station_id")
    assert df.loc["1", "dq_ok"]
    assert df.loc["2", "dq_excede_capacidad"] and df.loc["2", "dq_reporte_viejo"]
    assert df.loc["999", "dq_sin_catalogo"] and df.loc["999", "dq_fuera_servicio"]
    assert quality.summary(df.reset_index())["filas"] == 3


def test_weather_df():
    payload = {"hourly": {"time": ["2026-11-03T17:00", "2026-11-03T18:00"],
                          "temperature_2m": [21.5, 20.9], "precipitation": [0, 1.2],
                          "rain": [0, 1.2], "wind_speed_10m": [8, 9], "weather_code": [1, 61]}}
    df = weather.weather_df(payload)
    assert df["precipitation"].tolist() == [0, 1.2]


def test_particionado(tmp_path, monkeypatch):
    monkeypatch.setattr("ecobici.config.DATA_DIR", tmp_path)
    p = storage.write_bronze("station_status", STATUS, NOW)
    assert "dt=2026-11-03" in str(p) and "hour=18" in str(p)
    s = storage.write_silver("fact_station_status", gbfs.station_status_df(STATUS, NOW), NOW)
    assert len(pd.read_parquet(s)) == 3
