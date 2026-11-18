"""Costo real de la cuenta de AWS por servicio y por mes, desde Cost Explorer.

Uso:
    python scripts/costos_reales.py --salida E2/evidencias              # desde el primer despliegue (10/11/2026)
    python scripts/costos_reales.py --salida E2/evidencias --desde 2026-11-10

Notas:
- Muestra el costo de uso SIN descontar créditos (lo que costaría sin el plan gratuito).
- Cada llamada a la API de Cost Explorer cuesta 0.01 USD; este script hace 2.
- Si la cuenta nunca abrió Cost Explorer, hay que abrirlo una vez en la consola
  (Billing and Cost Management → Cost Explorer); los datos tardan hasta 24 h en aparecer.
"""
from __future__ import annotations

import argparse
from collections import defaultdict
from datetime import date, datetime, timedelta, timezone
from pathlib import Path

import boto3

# Solo uso: excluye créditos, reembolsos e impuestos para ver el costo real del consumo.
SOLO_USO = {"Dimensions": {"Key": "RECORD_TYPE", "Values": ["Usage"]}}


def consultar(ce, desde: str, hasta: str, granularidad: str, agrupar: bool):
    kw = dict(TimePeriod={"Start": desde, "End": hasta}, Granularity=granularidad,
              Metrics=["UnblendedCost"], Filter=SOLO_USO)
    if agrupar:
        kw["GroupBy"] = [{"Type": "DIMENSION", "Key": "SERVICE"}]
    return ce.get_cost_and_usage(**kw)["ResultsByTime"]


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--salida", required=True)
    ap.add_argument("--desde", default="2026-11-10")  # día del primer despliegue
    a = ap.parse_args()
    hoy = datetime.now(timezone.utc).date()
    hasta = (hoy + timedelta(days=1)).isoformat()   # el fin es exclusivo
    ce = boto3.client("ce", region_name="us-east-1")

    por_servicio = defaultdict(float)
    for periodo in consultar(ce, a.desde, hasta, "MONTHLY", agrupar=True):
        for g in periodo["Groups"]:
            por_servicio[g["Keys"][0]] += float(g["Metrics"]["UnblendedCost"]["Amount"])

    diario = consultar(ce, a.desde, hasta, "DAILY", agrupar=False)
    dias = [(p["TimePeriod"]["Start"], float(p["Total"]["UnblendedCost"]["Amount"])) for p in diario]
    total = sum(por_servicio.values())
    n_dias = max((hoy - date.fromisoformat(a.desde)).days + 1, 1)
    promedio_dia = total / n_dias

    md = [f"# Costo real en AWS\n",
          f"Periodo: {a.desde} a {hoy} ({n_dias} días) · Fuente: Cost Explorer · Costo de uso **sin descontar créditos** (USD).\n",
          f"**Total del periodo:** {total:.4f} USD · **Promedio diario:** {promedio_dia:.4f} USD · "
          f"**Proyección a 30 días:** {promedio_dia * 30:.2f} USD · **Proyección a 12 meses:** {promedio_dia * 365:.2f} USD\n",
          "## Por servicio\n", "| Servicio | Costo (USD) | % |", "|---|---|---|"]
    for svc, c in sorted(por_servicio.items(), key=lambda kv: -kv[1]):
        if c >= 0.00005:
            md.append(f"| {svc} | {c:.4f} | {c / total * 100 if total else 0:.1f}% |")
    md += ["\n## Por día\n", "| Día | Costo (USD) |", "|---|---|"]
    md += [f"| {d} | {c:.4f} |" for d, c in dias]
    md.append("\nIncluye todo lo de la cuenta: el despliegue inicial (10/11) y las pruebas pueden inflar algunos días.")

    out = Path(a.salida)
    out.mkdir(parents=True, exist_ok=True)
    (out / "costos_reales.md").write_text("\n".join(md) + "\n", encoding="utf-8")
    print(f"Total {total:.4f} USD en {n_dias} días; proyección 12 meses ≈ {promedio_dia * 365:.2f} USD")
    print(f"Escrito en {(out / 'costos_reales.md').resolve()}")


if __name__ == "__main__":
    main()
