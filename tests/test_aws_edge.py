"""Casos límite de la compactación con un S3 simulado vacío en cada prueba."""
from datetime import timedelta

import boto3
import pytest
from moto import mock_aws

from ecobici import config, storage
from ecobici.aws import compact
from test_aws import BUCKET, DAY, INFO, START, status_payload


@pytest.fixture
def s3(monkeypatch):
    for k, v in {"AWS_ACCESS_KEY_ID": "test", "AWS_SECRET_ACCESS_KEY": "test",
                 "AWS_DEFAULT_REGION": "us-east-1"}.items():
        monkeypatch.setenv(k, v)
    monkeypatch.setattr(config, "STORAGE", "s3")
    monkeypatch.setattr(config, "BUCKET", BUCKET)
    monkeypatch.setattr(storage, "_s3_client", None)
    with mock_aws():
        client = boto3.client("s3")
        client.create_bucket(Bucket=BUCKET)
        yield client
    storage._s3_client = None


def test_compactacion_usa_catalogo_previo(s3):
    storage.write_bronze("station_information", INFO, START - timedelta(days=2))
    storage.write_bronze("station_status", status_payload(START), START)
    res = compact.compact_day(DAY)
    assert res["station_status"]["catalogo_de"] == f"{DAY - timedelta(days=2)}"


def test_compactacion_sin_snapshots_falla_claro(s3):
    with pytest.raises(RuntimeError, match="No hay snapshots"):
        compact.compact_day(DAY)
