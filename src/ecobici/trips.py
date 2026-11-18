"""Viajes históricos de Ecobici: carga, limpieza y conteos por estación y hora.

Lleva a código probado la lógica validada en el notebook 01:
- Los nombres de columna cambian entre años; se normalizan a un esquema único.
- Los IDs de estación se leen como texto (conservan ceros a la izquierda, p. ej. "003")
  y coinciden con short_name del feed GBFS.
- Las fechas vienen en dd/mm/aaaa y en hora local de CDMX (sin zona horaria).
- Los archivos mensuales se organizan por fecha de llegada: el periodo se filtra
  por fecha de retiro, no por archivo.

Privacidad por diseño: clean() descarta edad y género después de calcular las reglas,
así que nada aguas abajo (conteos, features, modelo) toca datos personales.
"""
from __future__ import annotations

from pathlib import Path

import pandas as pd

from . import config

COLMAP = {
    "genero_usuario": "user_gender",
    "edad_usuario": "user_age",
    "bici": "bike_id",
    "ciclo_estacion_retiro": "station_out",
    "ciclo_estacionarribo": "station_in",
    "ciclo_estacion_arribo": "station_in",
    "fecha_retiro": "date_out",
    "hora_retiro": "time_out",
    "fecha_arribo": "date_in",
    "hora_arribo": "time_in",
}

# Reglas que invalidan un viaje como "demanda real" (no son datos demográficos).
DEMAND_RULES = [
    "dq_fecha_invalida",
    "dq_arribo_antes_de_retiro",
    "dq_viaje_menor_1min",
    "dq_viaje_mayor_24h",
    "dq_misma_estacion_menor_2min",
    "dq_duplicado",
]


def load_csv(path: str | Path) -> pd.DataFrame:
    path = Path(path)
    try:
        df = pd.read_csv(path, dtype=str)
    except UnicodeDecodeError:
        df = pd.read_csv(path, dtype=str, encoding="latin-1")
    df.columns = [c.strip().lower() for c in df.columns]
    df = df.rename(columns=COLMAP)
    missing = {"station_out", "station_in", "date_out", "time_out", "date_in", "time_in"} - set(df.columns)
    if missing:
        raise ValueError(f"{path.name}: faltan columnas {sorted(missing)}")
    df["source_file"] = path.name
    return df


def load_many(paths) -> pd.DataFrame:
    return pd.concat([load_csv(p) for p in paths], ignore_index=True)


def _parse_ts(d: pd.Series, t: pd.Series) -> pd.Series:
    s = d.str.strip() + " " + t.str.strip()
    ts = pd.to_datetime(s, format="%d/%m/%Y %H:%M:%S", errors="coerce")
    miss = ts.isna() & s.notna()
    if miss.any():  # respaldo para formatos de otros años
        ts[miss] = pd.to_datetime(s[miss], dayfirst=True, errors="coerce", format="mixed")
    return ts


def clean(raw: pd.DataFrame) -> pd.DataFrame:
    """Tipa, calcula reglas de calidad y descarta los datos personales.

    Devuelve una fila por viaje con: station_out, station_in, bike_id, ts_out, ts_in,
    duration_min, source_file, banderas dq_* y dq_ok (según DEMAND_RULES).
    """
    df = pd.DataFrame({
        "station_out": raw["station_out"].str.strip(),
        "station_in": raw["station_in"].str.strip(),
        "bike_id": raw.get("bike_id", pd.Series(pd.NA, index=raw.index)),
        "ts_out": _parse_ts(raw["date_out"], raw["time_out"]),
        "ts_in": _parse_ts(raw["date_in"], raw["time_in"]),
        "source_file": raw["source_file"],
    })
    df["duration_min"] = (df["ts_in"] - df["ts_out"]).dt.total_seconds() / 60
    age = pd.to_numeric(raw.get("user_age"), errors="coerce") if "user_age" in raw else pd.Series(pd.NA, index=raw.index)
    gender = raw.get("user_gender", pd.Series(pd.NA, index=raw.index))

    df["dq_fecha_invalida"] = df["ts_out"].isna() | df["ts_in"].isna()
    df["dq_arribo_antes_de_retiro"] = df["duration_min"] < 0
    df["dq_viaje_menor_1min"] = df["duration_min"].between(0, 1, inclusive="left")
    df["dq_viaje_mayor_24h"] = df["duration_min"] > 24 * 60
    df["dq_misma_estacion_menor_2min"] = (df["station_out"] == df["station_in"]) & (df["duration_min"] < 2)
    df["dq_duplicado"] = df.duplicated(subset=["bike_id", "ts_out", "station_out"], keep="first")
    # Reglas demográficas: se reportan, pero no invalidan el viaje como demanda.
    df["dq_edad_atipica"] = age.notna() & ~age.between(16, 85)
    df["dq_genero_no_reportado"] = gender.eq("?")

    df["dq_ok"] = ~df[DEMAND_RULES].any(axis=1)
    return df


def quality_summary(trips: pd.DataFrame) -> pd.DataFrame:
    flags = [c for c in trips.columns if c.startswith("dq_") and c != "dq_ok"]
    out = pd.DataFrame({"viajes": trips[flags].sum(), "pct": (trips[flags].mean() * 100).round(3)})
    out["invalida_demanda"] = out.index.isin(DEMAND_RULES)
    return out.sort_values("viajes", ascending=False)


def is_open_hour(hour: pd.Series) -> pd.Series:
    """Horario de operación (hora local): fuera de 1:00 a 4:59."""
    return ~hour.between(config.CLOSED_FROM_HOUR, config.CLOSED_UNTIL_HOUR - 1)


def hourly_counts(trips: pd.DataFrame, start: str, end: str, stations=None) -> pd.DataFrame:
    """Malla completa estación × hora (hora local) con retiros, arribos y neto.

    Incluye las horas sin viajes (en cero) y las horas de cierre, marcadas con
    en_horario=False, para que los rezagos de 24 y 168 h caigan en la misma hora.
    start/end en hora local, end exclusivo (p. ej. "2026-09-01", "2026-11-01").
    """
    ok = trips[trips["dq_ok"]]
    t0, t1 = pd.Timestamp(start), pd.Timestamp(end)
    out_ = ok[(ok["ts_out"] >= t0) & (ok["ts_out"] < t1)]
    in_ = ok[(ok["ts_in"] >= t0) & (ok["ts_in"] < t1)]

    if stations is None:
        stations = sorted(set(out_["station_out"]) | set(in_["station_in"]))
    hours = pd.date_range(t0, t1, freq="h", inclusive="left")
    grid = pd.MultiIndex.from_product([stations, hours], names=["station", "ts_hour"])

    ret = out_.groupby([out_["station_out"], out_["ts_out"].dt.floor("h")]).size()
    arr = in_.groupby([in_["station_in"], in_["ts_in"].dt.floor("h")]).size()
    ret.index.names = arr.index.names = ["station", "ts_hour"]

    df = pd.DataFrame(index=grid)
    df["retiros"] = ret.reindex(grid, fill_value=0).astype("int32")
    df["arribos"] = arr.reindex(grid, fill_value=0).astype("int32")
    df = df.reset_index()
    df["neto"] = df["arribos"] - df["retiros"]
    df["en_horario"] = is_open_hour(df["ts_hour"].dt.hour)
    return df
