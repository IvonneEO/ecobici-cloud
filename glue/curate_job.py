"""Job de Glue (Python shell): raw/bronze -> curated/silver en Parquet particionado por fecha.

Lo dispara un trigger programado de Glue a las 00:15 UTC y procesa el día UTC anterior.
Para reprocesar un día: parámetro --DATE 2026-11-12 (consola o `aws glue start-job-run`).

Reutiliza exactamente la misma lógica probada de ecobici.aws.compact, así local,
pruebas y nube aplican las mismas reglas. El paquete se sube como zip (glue/upload.sh)
y se importa directo del zip: no se instala, para no tocar el pandas/pyarrow que trae Glue.
"""
import json
import os
import sys
from datetime import date, datetime, timedelta, timezone

import boto3
from awsglue.utils import getResolvedOptions

args = getResolvedOptions(sys.argv, ["ECOBICI_BUCKET", "PACKAGE_KEY"])
bucket = args["ECOBICI_BUCKET"]

boto3.client("s3").download_file(bucket, args["PACKAGE_KEY"], "/tmp/ecobici.zip")
sys.path.insert(0, "/tmp/ecobici.zip")

# La configuración se lee al importar el paquete: se fija antes del import.
os.environ["ECOBICI_STORAGE"] = "s3"
os.environ["ECOBICI_BUCKET"] = bucket

from ecobici.aws import compact  # noqa: E402

if "--DATE" in sys.argv:
    day = date.fromisoformat(getResolvedOptions(sys.argv, ["DATE"])["DATE"])
else:
    day = datetime.now(timezone.utc).date() - timedelta(days=1)

result = compact.compact_day(day)
print(json.dumps(result, ensure_ascii=False, default=str))
