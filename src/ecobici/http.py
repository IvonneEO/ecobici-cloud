"""Cliente HTTP con reintentos y backoff exponencial."""
import time

import requests

from . import config


def get_json(url: str, params: dict | None = None) -> dict:
    last_err: Exception | None = None
    for attempt in range(config.HTTP_RETRIES):
        try:
            r = requests.get(
                url,
                params=params,
                headers={"User-Agent": config.USER_AGENT},
                timeout=config.HTTP_TIMEOUT_S,
            )
            r.raise_for_status()
            return r.json()
        except (requests.RequestException, ValueError) as err:
            last_err = err
            time.sleep(2**attempt)  # 1s, 2s, 4s
    raise RuntimeError(f"Fallo al consultar {url} tras {config.HTTP_RETRIES} intentos") from last_err
