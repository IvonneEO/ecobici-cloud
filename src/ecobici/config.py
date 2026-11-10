"""Configuración central. Todo se puede sobreescribir con variables de entorno."""
import os
from pathlib import Path

# Feed de descubrimiento GBFS de Ecobici (operado por Lyft).
# Si cambia, revisar https://ecobici.cdmx.gob.mx/en/open-data/
GBFS_DISCOVERY_URL = os.environ.get(
    "ECOBICI_GBFS_URL", "https://gbfs.mex.lyftbikes.com/gbfs/gbfs.json"
)

# Open-Meteo: uso no comercial, < 10,000 llamadas/día. Requiere atribución.
OPEN_METEO_URL = "https://api.open-meteo.com/v1/forecast"
CDMX_LAT, CDMX_LON = 19.4326, -99.1332

# Identificarse siempre: buena práctica con APIs públicas.
USER_AGENT = os.environ.get("ECOBICI_USER_AGENT", "ecobici-cloud-proyecto-academico/0.2")
HTTP_TIMEOUT_S = 15
HTTP_RETRIES = 3

# Almacenamiento con la misma estructura en local y en S3:
#   bronze/ = JSON crudo tal cual llegó (auditoría y reprocesamiento)
#   silver/ = Parquet limpio y tipado (lo que consulta Athena / el modelo)
# ECOBICI_STORAGE=local (default) escribe en DATA_DIR; =s3 escribe en ECOBICI_BUCKET.
STORAGE = os.environ.get("ECOBICI_STORAGE", "local")
BUCKET = os.environ.get("ECOBICI_BUCKET", "")
DATA_DIR = Path(os.environ.get("ECOBICI_DATA_DIR", "data"))
TIMEZONE = "America/Mexico_City"

# Horario de operación de Ecobici (hora local). Medido en la primera captura:
# sin viajes ni reportes confiables de 1:00 a 4:59.
CLOSED_FROM_HOUR = 1
CLOSED_UNTIL_HOUR = 5
