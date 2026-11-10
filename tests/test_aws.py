"""Pruebas del flujo en AWS con S3 simulado (moto): no usan internet ni una cuenta real.

Simulan un día completo de ingesta (720 snapshots + 24 consultas de clima)
y verifican la compactación diaria y que el esquema coincida con las tablas de Athena.
"""
import io
import os
import subprocess
import sys
from datetime import date, datetime, timedelta, timezone
from pathlib import Path

import boto3
import pandas as pd
import pyarrow.parquet as pq
import pytest
import yaml
from moto import mock_aws

from ecobici import config, storage
from ecobici.aws import compact, ingest

BUCKET = "ecobici-test"
DAY = date(2026, 11, 9)
START = datetime(2026, 11, 9, tzinfo=timezone.utc)
ROOT = Path(__file__).resolve().parents[1]

INFO = {"data": {"stations": [
    {"station_id": "1", "name": "Reforma", "short_name": "001", "lat": 19.43, "lon": -99.16, "capacity": 20},
    {"station_id": "2", "name": "Ámsterdam", "short_name": "002", "lat": 19.41, "lon": -99.17, "capacity": 10},
    {"station_id": "3", "name": "Caída", "short_name": "003", "lat": 19.40, "lon": -99.15, "capacity": 15},
]}}


def status_payload(ts: datetime) -> dict:
    t = int(ts.timestamp())
    return {"last_updated": t, "data": {"stations": [
        {"station_id": "1", "num_bikes_available": 5, "num_bikes_disabled": 0, "num_docks_available": 15,
         "num_docks_disabled": 0, "is_installed": 1, "is_renting": 1, "is_returning": 1, "last_reported": t - 30},
        {"station_id": "2", "num_bikes_available": 3, "num_bikes_disabled": 0, "num_docks_available": 7,
         "num_docks_disabled": 0, "is_installed": 1, "is_renting": 1, "is_returning": 1, "last_reported": t - 60},
        # Estación caída: repite el mismo last_reported todo el día.
        {"station_id": "3", "num_bikes_available": 0, "num_bikes_disabled": 0, "num_docks_available": 15,
         "num_docks_disabled": 0, "is_installed": 1, "is_renting": 1, "is_returning": 1,
         "last_reported": int(START.timestamp()) - 86400},
    ]}}


def weather_payload(fetched: datetime) -> dict:
    # Como Open-Meteo con past_days=1 y forecast_days=1: 48 horas, pasado y pronóstico.
    base = fetched.replace(minute=0, second=0) - timedelta(hours=24)
    horas = [base + timedelta(hours=i) for i in range(48)]
    return {"hourly": {
        "time": [h.strftime("%Y-%m-%dT%H:%M") for h in horas],
        "temperature_2m": [20.0] * 48,
        "precipitation": [fetched.hour / 10] * 48,   # identifica qué consulta ganó
        "rain": [0.0] * 48,
        "wind_speed_10m": [5.0] * 48,
        "weather_code": [1] * 48,
    }}


@pytest.fixture(scope="module")
def dia(request):
    """Un día completo de ingesta + compactación, simulado una sola vez para todo el módulo."""
    mp = pytest.MonkeyPatch()
    for k, v in {"AWS_ACCESS_KEY_ID": "test", "AWS_SECRET_ACCESS_KEY": "test",
                 "AWS_DEFAULT_REGION": "us-east-1"}.items():
        mp.setenv(k, v)
    mp.setattr(config, "STORAGE", "s3")
    mp.setattr(config, "BUCKET", BUCKET)
    mp.setattr(storage, "_s3_client", None)
    with mock_aws():
        client = boto3.client("s3")
        client.create_bucket(Bucket=BUCKET)
        simular_dia(mp)
        res = compact.compact_day(DAY)
        yield client, res
    mp.undo()
    storage._s3_client = None


def read_parquet(client, key: str) -> pd.DataFrame:
    return pd.read_parquet(io.BytesIO(client.get_object(Bucket=BUCKET, Key=key)["Body"].read()))


def simular_dia(monkeypatch):
    holder = {}
    monkeypatch.setattr("ecobici.feeds.fetch_feeds", lambda: (INFO, status_payload(holder["ts"])))
    monkeypatch.setattr("ecobici.feeds.fetch_weather", lambda: weather_payload(holder["ts"]))
    for i in range(720):
        holder["ts"] = START + timedelta(minutes=2 * i)
        ingest.run_status(holder["ts"])
    for h in range(24):
        holder["ts"] = START + timedelta(hours=h, minutes=1)
        ingest.run_weather(holder["ts"])


def test_bronze_un_snapshot_por_corrida_y_catalogo_diario(dia):
    assert len(storage.list_keys(f"bronze/station_status/dt={DAY}/")) == 720
    assert len(storage.list_keys(f"bronze/station_information/dt={DAY}/")) == 1  # catálogo 1 vez al día


def test_compactacion_resumen(dia):
    _, res = dia
    st = res["station_status"]
    assert st["snapshots"] == 720 and st["completitud_pct"] == 100.0 and st["huecos_mayores_5min"] == 0
    assert st["calidad"]["pct_ok_en_horario"] == pytest.approx(66.7, abs=0.1)


def test_reglas_de_calidad_en_el_dia(dia):
    client, _ = dia
    df = read_parquet(client, f"silver/fact_station_status/dt={DAY}/part-00000.parquet")
    assert len(df) == 720 * 3
    # La estación caída repite last_reported entre snapshots: NO es un duplicado.
    assert not df["dq_duplicado"].any()
    caida = df[df["station_id"] == "3"]
    assert caida["dq_reporte_viejo"].all() and not caida["dq_ok"].any()
    assert df[df["station_id"] != "3"]["dq_ok"].all()
    # De 1:00 a 4:59 en CDMX (7:00 a 10:59 UTC) está fuera de horario: 4 h × 30 snapshots × 3 estaciones.
    assert (~df["en_horario"]).sum() == 4 * 30 * 3


def test_clima_una_fila_por_hora_con_la_consulta_mas_reciente(dia):
    client, _ = dia
    w = read_parquet(client, f"silver/fact_weather_hourly/dt={DAY}/part-00000.parquet")
    assert len(w) == 24 and w["ts_hour"].is_unique
    assert w.loc[w["ts_hour"].dt.hour == 23, "precipitation"].iloc[0] == pytest.approx(2.3)


def test_compactacion_es_idempotente(dia):
    _, res = dia
    assert compact.compact_day(DAY) == res
    assert len(storage.list_keys(f"silver/fact_station_status/dt={DAY}/")) == 1


def test_timestamps_en_milisegundos_para_athena(dia):
    client, _ = dia
    body = client.get_object(Bucket=BUCKET, Key=f"silver/fact_station_status/dt={DAY}/part-00000.parquet")["Body"].read()
    assert str(pq.read_schema(io.BytesIO(body)).field("ingested_at").type) == "timestamp[ms, tz=UTC]"


def test_handler_rechaza_tarea_desconocida():
    with pytest.raises(ValueError):
        ingest.handler({"task": "otra"})


# ---------- Contratos con la infraestructura ----------

class _CfnLoader(yaml.SafeLoader):
    pass


_CfnLoader.add_multi_constructor("!", lambda loader, suffix, node: None)


def glue_columns(table: str) -> list[str]:
    tpl = yaml.load((ROOT / "template.yaml").read_text(encoding="utf-8"), Loader=_CfnLoader)
    for res in tpl["Resources"].values():
        if res["Type"] == "AWS::Glue::Table" and res["Properties"]["TableInput"]["Name"] == table:
            return [c["Name"] for c in res["Properties"]["TableInput"]["StorageDescriptor"]["Columns"]]
    raise KeyError(table)


@pytest.mark.parametrize("table", ["fact_station_status", "dim_station", "fact_weather_hourly"])
def test_esquema_parquet_coincide_con_tabla_athena(dia, table):
    client, _ = dia
    df = read_parquet(client, f"silver/{table}/dt={DAY}/part-00000.parquet")
    assert list(df.columns) == glue_columns(table)


def test_lambda_de_ingesta_no_carga_pandas():
    """La ingesta corre cada 2 min: debe ser ligera (sin pandas ni pyarrow)."""
    code = "import sys, ecobici.aws.ingest; print('pandas' in sys.modules or 'pyarrow' in sys.modules)"
    env = {**os.environ, "PYTHONPATH": str(ROOT / "src")}
    out = subprocess.run([sys.executable, "-c", code], capture_output=True, text=True, env=env, check=True)
    assert out.stdout.strip() == "False"
