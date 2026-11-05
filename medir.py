"""Resumen de la captura: pesos, snapshots, huecos y calidad."""
from pathlib import Path

import pandas as pd

DATA = Path("data")
TZ = "America/Mexico_City"


def size_mb(path: Path) -> float:
    return sum(f.stat().st_size for f in path.rglob("*") if f.is_file()) / 1e6


# 1. Pesos por capa y fuente
print("=== PESOS ===")
capas = {
    "bronze/station_status": DATA / "bronze" / "station_status",
    "bronze/station_information": DATA / "bronze" / "station_information",
    "bronze/weather": DATA / "bronze" / "weather",
    "silver/fact_station_status": DATA / "silver" / "fact_station_status",
    "silver/dim_station": DATA / "silver" / "dim_station",
    "silver/fact_weather_hourly": DATA / "silver" / "fact_weather_hourly",
}
pesos = {k: size_mb(v) for k, v in capas.items() if v.exists()}
for k, v in pesos.items():
    print(f"{k:<30} {v:8.2f} MB")

# 2. Snapshots, duración y huecos
files = sorted((DATA / "silver" / "fact_station_status").rglob("*.parquet"))
df = pd.concat([pd.read_parquet(f) for f in files], ignore_index=True)
snaps = pd.Series(sorted(df["ingested_at"].unique()))
snaps = pd.to_datetime(snaps, utc=True)
n = len(snaps)
horas = (snaps.iloc[-1] - snaps.iloc[0]).total_seconds() / 3600
print("\n=== CAPTURA ===")
print(f"Snapshots: {n}")
print(f"Inicio (CDMX): {snaps.iloc[0].tz_convert(TZ):%Y-%m-%d %H:%M}")
print(f"Fin (CDMX):    {snaps.iloc[-1].tz_convert(TZ):%Y-%m-%d %H:%M}")
print(f"Duración: {horas:.1f} horas")
print(f"Filas totales en silver: {len(df):,}")

gaps = snaps.diff().dt.total_seconds() / 60
huecos = pd.DataFrame({"desde": snaps.shift(1), "hasta": snaps, "min": gaps})[gaps > 5]
print(f"Huecos de más de 5 min: {len(huecos)}")
for _, h in huecos.iterrows():
    print(f"  {h['desde'].tz_convert(TZ):%H:%M} → {h['hasta'].tz_convert(TZ):%H:%M} ({h['min']:.0f} min)")
esperados = int(horas * 30) + 1
print(f"Completitud: {n} de ~{esperados} esperados ({n / esperados:.1%})")

# 3. Pesos extrapolados (a 720 snapshots/día)
print("\n=== PESO EXTRAPOLADO (720 snapshots/día) ===")
for capa in ["bronze/station_status", "silver/fact_station_status"]:
    if capa in pesos:
        por_snap_kb = pesos[capa] * 1000 / n
        dia_mb = por_snap_kb * 720 / 1000
        print(f"{capa:<30} {por_snap_kb:6.1f} KB/snapshot | {dia_mb:6.1f} MB/día | {dia_mb * 30 / 1000:5.2f} GB/mes")
total_dia = sum(pesos.get(c, 0) for c in capas) / max(horas, 1e-9) * 24
print(f"{'TOTAL (todas las capas)':<30} {total_dia:6.1f} MB/día | {total_dia * 30 / 1000:5.2f} GB/mes")

# 4. Calidad
print("\n=== CALIDAD ===")
por_snap = df.groupby("ingested_at")["dq_ok"].mean()
print(f"% promedio de estaciones válidas: {por_snap.mean():.1%} (mín {por_snap.min():.1%}, máx {por_snap.max():.1%})")
flags = [c for c in df.columns if c.startswith("dq_") and c != "dq_ok"]
prom = df.groupby("ingested_at")[flags].sum().mean().round(1).sort_values(ascending=False)
print("Estaciones marcadas por snapshot (promedio):")
for k, v in prom.items():
    print(f"  {k:<22} {v}")

viejo = df.groupby("station_id")["dq_reporte_viejo"].mean()
print(f"Estaciones sin reportar en el 100% de los snapshots (caídas): {(viejo == 1).sum()}")
print(f"Estaciones sin reportar de forma intermitente: {((viejo > 0) & (viejo < 1)).sum()}")
exc = df[df["dq_excede_capacidad"]]
print(f"Casos de exceso de capacidad: {len(exc)} en {exc['station_id'].nunique()} estaciones")

# 5. Clima
wf = list((DATA / "silver" / "fact_weather_hourly").rglob("*.parquet"))
print(f"\n=== CLIMA ===\nConsultas de clima guardadas: {len(wf)}")