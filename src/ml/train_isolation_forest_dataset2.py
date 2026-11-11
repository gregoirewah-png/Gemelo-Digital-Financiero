from __future__ import annotations
import sys
from pathlib import Path
sys.path.insert(0, str(Path(__file__).resolve().parents[2]))


import argparse
import json
import logging
import os
from pathlib import Path
import re
from datetime import datetime, timezone

import joblib
import numpy as np
import pandas as pd

from pyspark.sql import SparkSession
from pyspark.sql.types import (
    StructType, StructField, StringType, DoubleType, BooleanType, TimestampType,
)
from src.ml.credit_card_anomaly_features import fit_score
from src.common.spark import spark_session, read_lake, write_lake
from src.common.config import lake_path, safe_run_id
from src.common.observability import record_metrics
from src.validation.validate_gold_contract import validate_gold_contract

LOG = logging.getLogger("isolation_forest_dataset2")
BASE_DIR = Path(__file__).resolve().parents[2]


def get_spark() -> SparkSession:
    return spark_session("IsolationForest_Dataset2")


def run(args: argparse.Namespace) -> None:
    source_run_id = args.source_run_id or args.run_id
    source = args.input_path or lake_path("gold", "credit_card_balances", source_run_id)
    safe_id = safe_run_id(args.run_id)
    destination = lake_path("gold", "credit_card_anomalies", args.run_id)
    spark = None
    try:
        spark = get_spark()
        LOG.info("Leyendo Gold Dataset2 desde %s", source)
        spark_df = read_lake(spark, source)
        n_rows = spark_df.count()
        if n_rows > args.max_pandas_rows:
            raise ValueError(
                f"{n_rows} perfiles superan max_pandas_rows={args.max_pandas_rows}; "
                "no convertir a pandas; migrar a procesamiento distribuido."
            )
        if n_rows < 20:
            raise ValueError(f"Dataset insuficiente: {n_rows} registros")

        source_df = spark_df.orderBy("cust_id").toPandas()  # ~8950 filas: cabe en memoria razonablemente.
        scored, metrics, bundle = fit_score(
            source_df,
            contamination=args.contamination,
            seed=args.seed,
            n_estimators=args.n_estimators,
            stability_test=args.stability_test,
        )
        metrics["source_gold_path"] = source
        metrics["target_gold_path"] = destination
        metrics["run_id"] = args.run_id
        metrics["created_at_utc"] = datetime.now(timezone.utc).isoformat()

        # Tabla Gold específica para consumo de dashboard/IA.
        # No se incluyen los vectores de entrenamiento ni objetos Python.
        scored_at = datetime.now(timezone.utc).replace(tzinfo=None)
        cols = [
            "balance", "credit_limit", "purchases", "cash_advance", "payments",
            "minimum_payments", "purchases_trx", "prc_full_payment",
            "credit_utilization", "payment_to_balance", "minimum_payment_to_balance",
            "isolation_score", "anomaly_score", "decision_score",
        ]
        schema = StructType(
            [StructField("cust_id", StringType(), False)]
            + [StructField(name, DoubleType(), True) for name in cols]
            + [
                StructField("is_anomaly", BooleanType(), False),
                StructField("model_run_id", StringType(), False),
                StructField("scored_at", TimestampType(), False),
            ]
        )
        rows = []
        for item in scored.itertuples(index=False):
            row = [str(item.cust_id)]
            for name in cols:
                value = getattr(item, name)
                row.append(float(value) if pd.notna(value) and np.isfinite(value) else None)
            row.extend([bool(item.is_anomaly), str(args.run_id), scored_at])
            rows.append(tuple(row))

        out = spark.createDataFrame(rows, schema=schema)
        validation = validate_gold_contract(out, BASE_DIR / "schemas/gold_credit_card_anomalies.json")
        record_metrics(contract_validation=validation)
        # Escritura primero para evitar registrar éxito de una ejecución incompleta.
        LOG.info("Escribiendo anomalías en %s", destination)
        write_lake(out, destination)

        record_metrics(records_read=n_rows, records_written=n_rows, anomaly_count=metrics["n_anomalies"], anomaly_rate=metrics["anomaly_rate"])

        # El modelo debe vivir en un volumen persistente si se quiere reutilizar.
        model_dir = BASE_DIR / "models" / "isolation_forest_dataset2" / safe_id
        model_dir.mkdir(parents=True, exist_ok=True)
        joblib.dump(bundle, model_dir / "model.joblib")
        with (model_dir / "metrics.json").open("w", encoding="utf-8") as fh:
            json.dump(metrics, fh, ensure_ascii=False, indent=2)

        LOG.info("Procesados=%s, anomalías=%s (%.2f%%)",
                 metrics["n_rows"], metrics["n_anomalies"],
                 100 * metrics["anomaly_rate"])
        LOG.info("Modelo y métricas: %s", model_dir)
    finally:
        if spark is not None:
            try:
                spark.stop()
            except Exception as exc:
                LOG.warning("No se pudo cerrar Spark limpiamente: %s", exc)


def parse_args():
    parser = argparse.ArgumentParser(description="Isolation Forest sobre Gold Dataset2")
    parser.add_argument("--run-id", required=True)
    parser.add_argument("--source-run-id", default=None)
    parser.add_argument("--input-path", default=None,
                        help="Ruta alternativa del Parquet Gold de entrada")
    parser.add_argument("--contamination", type=float, default=0.02)
    parser.add_argument("--n-estimators", type=int, default=200)
    parser.add_argument("--seed", type=int, default=42)
    parser.add_argument("--max-pandas-rows", type=int, default=100_000)
    parser.add_argument("--stability-test", action="store_true")
    return parser.parse_args()


if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
    run(parse_args())
