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
USER_AGENT = os.environ.get("ECOBICI_USER_AGENT", "ecobici-cloud-proyecto-academico/0.1")
HTTP_TIMEOUT_S = 15
HTTP_RETRIES = 3

# Destino local que imita la estructura que tendrá en S3:
#   bronze/ = JSON crudo tal cual llegó (auditoría y reprocesamiento)
#   silver/ = Parquet limpio y tipado (lo que consulta Athena / el modelo)
DATA_DIR = Path(os.environ.get("ECOBICI_DATA_DIR", "data"))
TIMEZONE = "America/Mexico_City"
