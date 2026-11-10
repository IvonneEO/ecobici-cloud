"""Cliente HTTP con reintentos y backoff exponencial.

Usa solo la librería estándar para que la Lambda de ingesta no necesite
dependencias externas (paquete pequeño y arranque rápido).
"""
from __future__ import annotations

import json
import time
import urllib.error
import urllib.parse
import urllib.request

from . import config


def get_json(url: str, params: dict | None = None) -> dict:
    if params:
        url = f"{url}?{urllib.parse.urlencode(params)}"
    req = urllib.request.Request(url, headers={"User-Agent": config.USER_AGENT})
    last_err: Exception | None = None
    for attempt in range(config.HTTP_RETRIES):
        try:
            with urllib.request.urlopen(req, timeout=config.HTTP_TIMEOUT_S) as r:
                return json.loads(r.read().decode("utf-8"))
        except (urllib.error.URLError, TimeoutError, ValueError) as err:
            last_err = err
            if attempt < config.HTTP_RETRIES - 1:
                time.sleep(2**attempt)  # 1s, 2s
    raise RuntimeError(f"Fallo al consultar {url} tras {config.HTTP_RETRIES} intentos") from last_err
