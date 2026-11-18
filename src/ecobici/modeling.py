"""Modelos y evaluación del pronóstico de demanda por estación y hora."""
from __future__ import annotations

import numpy as np
import pandas as pd

FEATURES = [
    "hora", "dia_semana", "fin_de_semana", "festivo", "dia_laboral", "quincena",
    "retiros_lag_1h", "retiros_lag_24h", "retiros_lag_168h", "retiros_media_3h",
    "arribos_lag_1h", "arribos_lag_24h", "arribos_lag_168h", "arribos_media_3h",
    "retiros_perfil", "arribos_perfil",
    "temperature_2m", "precipitation", "llueve",
]


def temporal_split(df: pd.DataFrame, test_start: str):
    cut = pd.Timestamp(test_start)
    return df[df["ts_hour"] < cut].copy(), df[df["ts_hour"] >= cut].copy()


def metrics(y, yhat) -> dict:
    y, yhat = np.asarray(y, dtype=float), np.asarray(yhat, dtype=float)
    err = yhat - y
    total = np.abs(y).sum()
    return {
        "MAE": float(np.abs(err).mean()),
        "RMSE": float(np.sqrt((err ** 2).mean())),
        "WAPE_%": float(np.abs(err).sum() / total * 100) if total else float("nan"),
        "sesgo": float(err.mean()),   # >0 sobreestima, <0 subestima
        "n": int(len(y)),
    }


def evaluate(test: pd.DataFrame, preds: dict[str, pd.Series], target: str, segments: dict[str, pd.Series]) -> pd.DataFrame:
    """Tabla modelo × segmento con las métricas de `metrics`."""
    rows = []
    for seg, mask in segments.items():
        for name, p in preds.items():
            rows.append({"modelo": name, "segmento": seg, **metrics(test.loc[mask, target], p[mask])})
    return pd.DataFrame(rows)


def fit_hgb(train: pd.DataFrame, target: str, features=FEATURES, random_state: int = 42):
    """Gradient boosting con pérdida Poisson: adecuada para conteos (no negativos, sesgados)."""
    from sklearn.ensemble import HistGradientBoostingRegressor

    model = HistGradientBoostingRegressor(
        loss="poisson",
        learning_rate=0.08,
        max_iter=400,
        max_leaf_nodes=63,
        min_samples_leaf=50,
        l2_regularization=1.0,
        early_stopping=True,
        validation_fraction=0.1,
        n_iter_no_change=20,
        random_state=random_state,
    )
    X = train[features].astype("float32")
    model.fit(X, train[target])
    return model


def predict(model, df: pd.DataFrame, features=FEATURES) -> pd.Series:
    return pd.Series(model.predict(df[features].astype("float32")), index=df.index)
