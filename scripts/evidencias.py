"""Genera las evidencias del entregable E2 a partir de la cuenta de AWS real.

Uso (desde la raíz del proyecto, con credenciales configuradas):
    python scripts/evidencias.py --salida E2_EQxx/evidencias            # día = ayer (UTC)
    python scripts/evidencias.py --salida E2_EQxx/evidencias --dia 2026-11-12

Escribe:
    athena_consultas.md   3 consultas con bytes escaneados, tiempo y costo
    capas_s3.md           objetos y tamaño por capa y clase de almacenamiento
    ciclo_de_vida.json    reglas de ciclo de vida vigentes del bucket
    glue_corridas.md      últimas corridas del Job de Glue
    ingesta.md            horarios (triggers) de la ingesta y del job
"""
from __future__ import annotations

import argparse
import json
import time
from collections import defaultdict
from datetime import datetime, timedelta, timezone
from pathlib import Path

import boto3

STACK = "ecobici-cloud"
WORKGROUP = "ecobici-cloud"
PRECIO_TB_USD = 5.0          # Athena: 5 USD por TB escaneado
MIN_BYTES = 10 * 1024 ** 2   # Athena cobra mínimo 10 MB por consulta


def outputs(cf) -> dict:
    out = cf.describe_stacks(StackName=STACK)["Stacks"][0]["Outputs"]
    return {o["OutputKey"]: o["OutputValue"] for o in out}


def consultas(dia: str) -> list[tuple[str, str, str]]:
    return [
        (
            "Q1. Calidad por hora de un día (con filtro de partición)",
            "Lee solo la partición del día y 2 columnas: el caso eficiente.",
            f"""SELECT hour(ingested_at AT TIME ZONE 'America/Mexico_City') AS hora_cdmx,
       count(DISTINCT ingested_at) AS snapshots,
       round(avg(IF(dq_ok, 1.0, 0.0)) * 100, 1) AS pct_validas
FROM ecobici.fact_station_status
WHERE dt = '{dia}'
GROUP BY 1
ORDER BY 1""",
        ),
        (
            "Q2. La misma consulta sin filtro de partición",
            "Recorre todos los días capturados: muestra cuánto ahorra particionar por fecha.",
            """SELECT hour(ingested_at AT TIME ZONE 'America/Mexico_City') AS hora_cdmx,
       count(DISTINCT ingested_at) AS snapshots,
       round(avg(IF(dq_ok, 1.0, 0.0)) * 100, 1) AS pct_validas
FROM ecobici.fact_station_status
GROUP BY 1
ORDER BY 1""",
        ),
        (
            "Q3. Estaciones que más tiempo pasaron vacías (negocio, con JOIN)",
            "Une hechos con el catálogo; solo horario de operación y registros válidos.",
            f"""SELECT s.short_name, s.name,
       round(avg(IF(f.num_bikes_available = 0, 1.0, 0.0)) * 100, 1) AS pct_tiempo_vacia
FROM ecobici.fact_station_status f
JOIN ecobici.dim_station s ON s.station_id = f.station_id AND s.dt = f.dt
WHERE f.dt = '{dia}' AND f.en_horario AND f.dq_ok
GROUP BY 1, 2
ORDER BY pct_tiempo_vacia DESC
LIMIT 10""",
        ),
    ]


def correr(athena, sql: str) -> dict:
    qid = athena.start_query_execution(QueryString=sql, WorkGroup=WORKGROUP)["QueryExecutionId"]
    while True:
        q = athena.get_query_execution(QueryExecutionId=qid)["QueryExecution"]
        if q["Status"]["State"] in ("SUCCEEDED", "FAILED", "CANCELLED"):
            break
        time.sleep(1)
    if q["Status"]["State"] != "SUCCEEDED":
        raise RuntimeError(f"Consulta {qid} {q['Status']['State']}: {q['Status'].get('StateChangeReason')}")
    rows = athena.get_query_results(QueryExecutionId=qid, MaxResults=11)["ResultSet"]["Rows"]
    tabla = [[c.get("VarCharValue", "") for c in r["Data"]] for r in rows]
    b = q["Statistics"]["DataScannedInBytes"]
    return {
        "id": qid,
        "bytes": b,
        "ms": q["Statistics"]["EngineExecutionTimeInMillis"],
        "costo": max(b, MIN_BYTES) / 1024 ** 4 * PRECIO_TB_USD,
        "tabla": tabla,
    }


def md_tabla(filas: list[list[str]]) -> str:
    if not filas:
        return "_(sin filas)_"
    head, *body = filas
    out = ["| " + " | ".join(head) + " |", "|" + "---|" * len(head)]
    out += ["| " + " | ".join(r) + " |" for r in body]
    return "\n".join(out)


def humano(b: float) -> str:
    for u in ["B", "KB", "MB", "GB"]:
        if b < 1024:
            return f"{b:,.1f} {u}"
        b /= 1024
    return f"{b:,.1f} TB"


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--salida", required=True)
    ap.add_argument("--dia", default=(datetime.now(timezone.utc).date() - timedelta(days=1)).isoformat())
    a = ap.parse_args()
    out = Path(a.salida)
    out.mkdir(parents=True, exist_ok=True)
    ahora = datetime.now(timezone.utc).strftime("%Y-%m-%d %H:%M UTC")

    o = outputs(boto3.client("cloudformation"))
    bucket = o["BucketName"]
    s3, athena, glue = boto3.client("s3"), boto3.client("athena"), boto3.client("glue")

    # 1) Athena
    md = [f"# Consultas en Athena\n\nGenerado: {ahora} · Workgroup `{WORKGROUP}` (tope de 1 GB por consulta) · Día: `{a.dia}`\n",
          "Costo = max(bytes escaneados, 10 MB) × 5 USD/TB, como cobra Athena.\n",
          "| Consulta | Bytes escaneados | Tiempo | Costo (USD) | Query ID |", "|---|---|---|---|---|"]
    detalle = []
    for titulo, porque, sql in consultas(a.dia):
        r = correr(athena, sql)
        md.append(f"| {titulo} | {r['bytes']:,} ({humano(r['bytes'])}) | {r['ms'] / 1000:.1f} s | {r['costo']:.8f} | `{r['id']}` |")
        detalle.append(f"\n## {titulo}\n\n{porque}\n\n```sql\n{sql}\n```\n\n**Bytes escaneados:** {r['bytes']:,} · "
                       f"**Tiempo:** {r['ms'] / 1000:.1f} s · **Costo:** {r['costo']:.8f} USD\n\nPrimeras filas:\n\n{md_tabla(r['tabla'])}\n")
        print(f"{titulo}: {humano(r['bytes'])}")
    (out / "athena_consultas.md").write_text("\n".join(md) + "\n" + "".join(detalle), encoding="utf-8")

    # 2) Capas de S3
    agg = defaultdict(lambda: [0, 0])
    for page in s3.get_paginator("list_objects_v2").paginate(Bucket=bucket):
        for obj in page.get("Contents", []):
            capa = obj["Key"].split("/", 1)[0] + "/"
            agg[(capa, obj.get("StorageClass", "STANDARD"))][0] += 1
            agg[(capa, obj.get("StorageClass", "STANDARD"))][1] += obj["Size"]
    md = [f"# Capas del lago en S3\n\nGenerado: {ahora} · Bucket `{bucket}`\n",
          "| Capa | Clase de almacenamiento | Objetos | Tamaño |", "|---|---|---|---|"]
    md += [f"| `{c}` | {k} | {n:,} | {humano(b)} |" for (c, k), (n, b) in sorted(agg.items())]
    (out / "capas_s3.md").write_text("\n".join(md) + "\n", encoding="utf-8")

    # 3) Ciclo de vida
    lc = s3.get_bucket_lifecycle_configuration(Bucket=bucket)["Rules"]
    (out / "ciclo_de_vida.json").write_text(json.dumps(lc, indent=2, default=str, ensure_ascii=False), encoding="utf-8")

    # 4) Corridas del Job de Glue
    job = o["CurateJobName"]
    runs = glue.get_job_runs(JobName=job, MaxResults=10)["JobRuns"]
    md = [f"# Corridas del Job de Glue `{job}`\n\nGenerado: {ahora}\n",
          "| Inicio (UTC) | Estado | Duración | Argumentos | Error |", "|---|---|---|---|---|"]
    for r in runs:
        args = {k: v for k, v in r.get("Arguments", {}).items() if k == "--DATE"}
        md.append(f"| {r['StartedOn']:%Y-%m-%d %H:%M} | {r['JobRunState']} | {r.get('ExecutionTime', 0)} s | "
                  f"{args or 'programada'} | {r.get('ErrorMessage', '')[:120]} |")
    (out / "glue_corridas.md").write_text("\n".join(md) + "\n", encoding="utf-8")

    # 5) Triggers de ingesta y procesamiento
    sch = boto3.client("scheduler")
    md = [f"# Triggers (eventos que disparan la ingesta y el procesamiento)\n\nGenerado: {ahora}\n",
          "| Servicio | Nombre | Expresión | Estado | Destino |", "|---|---|---|---|---|"]
    ingest = o["IngestFunctionName"]
    for page in sch.get_paginator("list_schedules").paginate():
        for s in page["Schedules"]:
            d = sch.get_schedule(Name=s["Name"], GroupName=s["GroupName"])
            if not d["Target"]["Arn"].endswith(ingest):
                continue
            md.append(f"| EventBridge Scheduler | {s['Name']} | `{d['ScheduleExpression']}` | {d['State']} | "
                      f"{d['Target']['Arn'].split(':')[-1]} `{d['Target'].get('Input', '')}` |")
    trig = glue.get_trigger(Name=f"{STACK}-curate-diario")["Trigger"]
    md.append(f"| Glue Trigger | {trig['Name']} | `{trig['Schedule']}` | {trig['State']} | {trig['Actions'][0]['JobName']} |")
    (out / "ingesta.md").write_text("\n".join(md) + "\n", encoding="utf-8")

    print(f"Evidencias en {out.resolve()}")


if __name__ == "__main__":
    main()
