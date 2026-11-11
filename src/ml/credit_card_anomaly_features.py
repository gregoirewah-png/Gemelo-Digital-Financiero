"""Construcción de características para Isolation Forest sobre Gold Dataset 2.

No hace consultas de red: permite pruebas unitarias con pandas.
"""
from __future__ import annotations

import numpy as np
import pandas as pd
import math
from sklearn.ensemble import IsolationForest
from sklearn.impute import SimpleImputer

BASE_COLUMNS = [
    "balance", "credit_limit", "purchases", "cash_advance", "payments",
    "minimum_payments", "purchases_trx", "prc_full_payment",
]
RATIO_COLUMNS = [
    "credit_utilization", "payment_to_balance", "minimum_payment_to_balance"
]
MODEL_COLUMNS = BASE_COLUMNS + RATIO_COLUMNS
LOG_COLUMNS = [
    "balance", "credit_limit", "purchases", "cash_advance", "payments",
    "minimum_payments", "purchases_trx", "credit_utilization",
    "payment_to_balance", "minimum_payment_to_balance",
]


def build_features(data: pd.DataFrame) -> tuple[pd.DataFrame, pd.DataFrame, dict]:
    """Devuelve (datos + ratios originales, características preparadas, diagnóstico).

    No modifica el DataFrame original. En cocientes con denominador cero,
    conserva NaN para que el imputador lo trate sin inventar infinito.
    """
    df = data.copy()
    df.columns = [str(c).strip().lower() for c in df.columns]

    required = ["cust_id", *BASE_COLUMNS]
    absent = sorted(set(required) - set(df.columns))
    if absent:
        raise ValueError(
            f"Gold Dataset2 no contiene {absent}. "
            "Verifica que Silver->Gold conserve CUST_ID y las variables de pago."
        )

    df["cust_id"] = df["cust_id"].astype("string").str.strip()
    if df["cust_id"].isna().any() or (df["cust_id"] == "").any():
        raise ValueError("cust_id no debe ser NULL ni vacío en Gold")
    if df["cust_id"].duplicated().any():
        raise ValueError("Hay cust_id duplicados: lee solo una partición run_id de Gold")
    if df.empty:
        raise ValueError("No hay registros para entrenar Isolation Forest")

    for name in BASE_COLUMNS:
        df[name] = pd.to_numeric(df[name], errors="coerce")

    # Errores de integridad: deben solucionarse upstream en Silver/Gold.
    critical = ["balance", "credit_limit", "purchases", "cash_advance", "payments"]
    for name in critical:
        if df[name].isna().any():
            raise ValueError(f"{name} contiene nulos/no numéricos en Gold")
    if (df["credit_limit"] <= 0).any():
        raise ValueError("credit_limit debe ser mayor que cero")
    for name in BASE_COLUMNS:
        if (df[name].dropna() < 0).any():
            raise ValueError(f"{name} contiene valores negativos; resolver en Silver")
    bad_prc = df["prc_full_payment"].notna() & ~df["prc_full_payment"].between(0, 1)
    if bad_prc.any():
        raise ValueError("prc_full_payment debe estar entre 0 y 1")

    balance_positive = df["balance"].where(df["balance"] > 0)
    df["credit_utilization"] = df["balance"] / df["credit_limit"]
    df["payment_to_balance"] = df["payments"] / balance_positive
    df["minimum_payment_to_balance"] = df["minimum_payments"] / balance_positive

    X = df[MODEL_COLUMNS].astype("float64").copy()
    X = X.replace([np.inf, -np.inf], np.nan)
    nan_before = {c: int(X[c].isna().sum()) for c in X.columns}
    if X.isna().all().any():
        empty = list(X.columns[X.isna().all()])
        raise ValueError(f"Características sin ningún valor disponible: {empty}")
    X.loc[:, LOG_COLUMNS] = np.log1p(X[LOG_COLUMNS])

    # El árbol no necesita StandardScaler: log1p cambia las distancias
    # relativas de importes sesgados; no se eliminan los valores atípicos.
    diagnostic = {
        "missing_features_before_imputation": nan_before,
        "feature_columns": MODEL_COLUMNS,
        "log1p_columns": LOG_COLUMNS,
        "zero_balance_rows": int((df["balance"] == 0).sum()),
    }
    return df, X, diagnostic


def fit_score(
    data: pd.DataFrame,
    contamination: float = 0.02,
    seed: int = 42,
    n_estimators: int = 200,
    stability_test: bool = False,
) -> tuple[pd.DataFrame, dict, dict]:
    """Entrena y puntúa registros; sin afirmar que los atípicos son fraude."""
    if not 0 < contamination <= 0.5:
        raise ValueError("contamination debe estar en el intervalo (0, 0.5]")

    prepared, X, diagnosis = build_features(data)
    if len(X) < 20:
        raise ValueError("Se necesitan al menos 20 perfiles para este experimento")
    # En Gold puede haber mínimos nulos legítimos; se imputan para entrenamiento.
    imputer = SimpleImputer(strategy="median", add_indicator=True)
    matrix = imputer.fit_transform(X)

    model = IsolationForest(
        n_estimators=n_estimators,
        contamination=contamination,
        max_samples="auto",
        random_state=seed,
        n_jobs=1,
    )
    model.fit(matrix)
    raw_scores = model.score_samples(matrix)
    decisions = model.decision_function(matrix)
    predicted = model.predict(matrix)

    scored = prepared.copy()
    scored["isolation_score"] = raw_scores       # Menor = más anómalo.
    scored["anomaly_score"] = -raw_scores        # Mayor = más anómalo; NO probabilidad.
    scored["decision_score"] = decisions        # Negativo = fuera del umbral.
    scored["is_anomaly"] = (predicted == -1)

    metrics = {
        "dataset": "Dataset2 credit_card_balances",
        "model": "sklearn.IsolationForest",
        "n_rows": len(scored),
        "n_anomalies": int(scored["is_anomaly"].sum()),
        "anomaly_rate": float(scored["is_anomaly"].mean()),
        "contamination": contamination,
        "threshold_offset": float(model.offset_),
        "random_state": seed,
        "n_estimators": n_estimators,
        "anomaly_score_p50": float(scored["anomaly_score"].median()),
        "anomaly_score_p95": float(scored["anomaly_score"].quantile(.95)),
        "anomaly_score_p99": float(scored["anomaly_score"].quantile(.99)),
        **diagnosis,
    }

    if stability_test:
        # Jaccard de los N casos más anómalos al variar las semillas.
        top_k = max(1, math.ceil(contamination * len(scored)))
        ref_top = set(np.argsort(-scored["anomaly_score"].to_numpy())[:top_k])
        jaccards = []
        for alternative_seed in [seed + 1, seed + 2]:
            other = IsolationForest(
                n_estimators=n_estimators,
                contamination=contamination,
                max_samples="auto",
                random_state=alternative_seed,
                n_jobs=1,
            ).fit(matrix)
            other_top = set(np.argsort(other.score_samples(matrix))[:top_k])
            jaccards.append(len(ref_top & other_top) / len(ref_top | other_top))
        metrics["top_k_stability_jaccard_mean"] = float(np.mean(jaccards))

    bundle = {
        "model": model,
        "imputer": imputer,
        "features": MODEL_COLUMNS,
        "transformations": diagnosis,
    }
    return scored, metrics, bundle

