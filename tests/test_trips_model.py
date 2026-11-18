"""Pruebas de viajes, features y modelado (datos sintéticos, sin archivos reales)."""
import numpy as np
import pandas as pd
import pytest

from ecobici import features, modeling, trips
from synthetic_trips import make, write


@pytest.fixture(scope="module")
def raw(tmp_path_factory):
    folder = tmp_path_factory.mktemp("raw_trips")
    write(folder, n_stations=12, start="2026-09-01", end="2026-10-01")
    return trips.load_many(sorted(folder.glob("*.csv")))


@pytest.fixture(scope="module")
def clean(raw):
    return trips.clean(raw)


def test_carga_normaliza_columnas_y_conserva_ceros(raw):
    assert {"station_out", "station_in", "date_out", "time_in"} <= set(raw.columns)
    assert raw["station_out"].str.len().eq(3).all()          # "001", no "1"


def test_limpieza_descarta_datos_personales(clean):
    assert "user_gender" not in clean.columns and "user_age" not in clean.columns


def test_reglas_de_calidad(clean):
    assert clean.loc[0, "dq_fecha_invalida"]
    assert clean.loc[1, "dq_arribo_antes_de_retiro"]
    assert clean.loc[2, "dq_viaje_mayor_24h"]
    assert clean.loc[3, "dq_viaje_menor_1min"]
    assert not clean.loc[[0, 1, 2, 3], "dq_ok"].any()
    # Género "?" se reporta pero no invalida el viaje como demanda.
    noreport = clean["dq_genero_no_reportado"] & ~clean[trips.DEMAND_RULES].any(axis=1)
    assert noreport.any() and clean.loc[noreport, "dq_ok"].all()


def test_malla_horaria_completa_y_conteos(clean):
    g = trips.hourly_counts(clean, "2026-09-01", "2026-10-01")
    n_st = g["station"].nunique()
    assert len(g) == n_st * 30 * 24
    ok = clean[clean["dq_ok"] & (clean["ts_out"] >= "2026-09-01") & (clean["ts_out"] < "2026-10-01")]
    assert g["retiros"].sum() == len(ok)
    assert (g.loc[~g["en_horario"], "retiros"] == 0).all()       # sistema cerrado de 1 a 4:59
    assert (g["neto"] == g["arribos"] - g["retiros"]).all()


def test_rezagos_por_estacion_sin_mezclar(clean):
    g = features.add_lags(trips.hourly_counts(clean, "2026-09-01", "2026-10-01"))
    for st, d in g.groupby("station"):
        d = d.reset_index(drop=True)
        assert d["retiros_lag_24h"].iloc[:24].isna().all()        # no hereda de otra estación
        assert (d["retiros_lag_24h"].iloc[24:].values == d["retiros"].iloc[:-24].values).all()
        assert d.loc[5, "retiros_media_3h"] == pytest.approx(d.loc[2:4, "retiros"].mean())


def test_calendario():
    df = pd.DataFrame({"ts_hour": pd.to_datetime(["2026-09-16 08:00", "2026-09-15 08:00", "2026-09-19 08:00"])})
    df = features.add_calendar(df)
    assert df["festivo"].tolist() == [True, False, False]
    assert df["quincena"].tolist() == [False, True, False]
    assert df["hora_pico"].tolist() == [False, True, False]   # festivo y sábado no son pico laboral


def test_perfil_no_usa_datos_de_prueba(clean):
    """Si cambian los datos del periodo de prueba, el perfil no debe cambiar."""
    g = features.add_calendar(trips.hourly_counts(clean, "2026-09-01", "2026-10-01"))
    p1 = features.station_profile(modeling.temporal_split(g, "2026-09-20")[0])
    g2 = g.copy()
    g2.loc[g2["ts_hour"] >= "2026-09-20", "retiros"] *= 100
    p2 = features.station_profile(modeling.temporal_split(g2, "2026-09-20")[0])
    pd.testing.assert_frame_equal(p1, p2)
    # Y si se usara la malla completa, sí cambiaría (la prueba detecta la fuga).
    assert not features.station_profile(g).equals(features.station_profile(g2))


def test_metricas_conocidas():
    m = modeling.metrics([0, 2, 4], [1, 2, 2])
    assert m["MAE"] == pytest.approx(1.0)
    assert m["WAPE_%"] == pytest.approx(50.0)
    assert m["sesgo"] == pytest.approx(-1 / 3)


def test_modelo_supera_baseline_en_sintetico(tmp_path):
    folder = tmp_path / "raw"
    weather, _ = write(folder, n_stations=15, start="2026-09-01", end="2026-10-15")
    t = trips.clean(trips.load_many(sorted(folder.glob("*.csv"))))
    g = trips.hourly_counts(t, "2026-09-01", "2026-10-15")
    g = features.add_weather(features.add_lags(features.add_calendar(g)), weather)
    train, test = modeling.temporal_split(g, "2026-10-01")
    prof = features.station_profile(train)
    train, test = features.add_profile(train, prof), features.add_profile(test, prof)
    train, test = train[train["en_horario"]], test[test["en_horario"]]

    model = modeling.fit_hgb(train, "retiros")
    pred = modeling.predict(model, test)
    m_model = modeling.metrics(test["retiros"], pred)
    m_base = modeling.metrics(test["retiros"], test["retiros_perfil"].fillna(0))
    assert np.isfinite(m_model["MAE"]) and (pred >= 0).all()
    # Con lluvia en los datos sintéticos, el modelo con clima debe ganarle al perfil.
    assert m_model["MAE"] < m_base["MAE"]
