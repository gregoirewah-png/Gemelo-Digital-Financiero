import argparse
import json
import os
import re
from datetime import datetime, timezone
from pathlib import Path

from pyspark.ml.clustering import KMeans
from pyspark.ml.evaluation import ClusteringEvaluator
from pyspark.ml.feature import StandardScaler, VectorAssembler
from pyspark.sql import SparkSession, functions as F
from pyspark.sql.types import DoubleType, IntegerType
from pyspark.storagelevel import StorageLevel

import sys
from pathlib import Path
sys.path.insert(0, str(Path(__file__).resolve().parents[2]))

from src.common.spark import spark_session, read_lake, write_lake
from src.common.config import lake_path

AMOUNTS = ["purchases", "oneoff_purchases", "installments_purchases", "cash_advance"]
FREQUENCIES = ["purchases_frequency", "oneoff_purchases_frequency", "purchases_installments_frequency", "cash_advance_frequency", "prc_full_payment", "balance_frequency"]
FEATURES = ["purchases", "oneoff_purchases", "installments_purchases", "cash_advance", *FREQUENCIES, "tenure", "credit_utilization", "installment_purchase_ratio", "cash_advance_share"]
REQUIRED = sorted(set(FEATURES) - {"credit_utilization", "installment_purchase_ratio", "cash_advance_share"} | {"cust_id", "balance", "credit_limit"})


def session():
    return spark_session("GoldKMeansDataset2")


def prepare(df):
    df = df.toDF(*[c.lower() for c in df.columns])
    missing = sorted(set(REQUIRED) - set(df.columns))
    if missing:
        raise ValueError(f"Columnas ausentes en Silver: {missing}")
    if df.filter(F.col("cust_id").isNull() | (F.trim("cust_id") == "")).limit(1).count():
        raise ValueError("Hay cust_id nulos o vacíos")
    if df.groupBy("cust_id").count().filter("count > 1").limit(1).count():
        raise ValueError("cust_id duplicado: la segmentación requiere un registro por cliente")
    for name in REQUIRED:
        if name != "cust_id":
            df = df.withColumn(name, F.col(name).cast("double"))
    # Datos negativos y valores no finitos son errores de calidad, no se ocultan.
    for name in REQUIRED:
        if name == "cust_id":
            continue
        if df.filter(F.col(name).isNotNull() & (F.isnan(name) | (F.col(name) < 0))).limit(1).count():
            raise ValueError(f"Valor negativo o NaN en {name}")
    if df.filter(F.col("credit_limit").isNull() | (F.col("credit_limit") <= 0)).limit(1).count():
        raise ValueError("credit_limit debe ser positivo")
    for name in FREQUENCIES:
        if df.filter(F.col(name).isNotNull() & (F.col(name) > 1)).limit(1).count():
            raise ValueError(f"Frecuencia fuera de rango: {name}")
    df = (df
        .withColumn("credit_utilization", F.col("balance") / F.col("credit_limit"))
        .withColumn("installment_purchase_ratio", F.when(F.col("purchases") > 0, F.col("installments_purchases") / F.col("purchases")).otherwise(F.lit(0.0)))
        .withColumn("cash_advance_share", F.when(F.col("purchases") + F.col("cash_advance") > 0, F.col("cash_advance") / (F.col("purchases") + F.col("cash_advance"))).otherwise(F.lit(0.0))))
    # Los nulos Silver se imputan con mediana por variable (no con 0 arbitrario).
    for name in FEATURES:
        if df.filter(F.col(name).isNull()).limit(1).count():
            median = df.approxQuantile(name, [0.5], 0.01)
            if not median:
                raise ValueError(f"Característica completamente nula: {name}")
            df = df.fillna({name: float(median[0])})
    # Log1p en montos/tenure y utilización, después estandarización.
    log_features = set(AMOUNTS + ["tenure", "credit_utilization"])
    prepared = []
    for name in FEATURES:
        out = f"f_{name}"
        df = df.withColumn(out, F.log1p(F.col(name)) if name in log_features else F.col(name))
        prepared.append(out)
    return df, prepared


def run(args):
    spark = session()
    prepared_df = None
    scaled = None
    try:
        safe_id = re.sub(r"[^a-zA-Z0-9_.-]", "_", args.run_id)
        silver_path = lake_path("silver", "credit_card_balances", args.run_id)
        gold_path = lake_path("gold", "credit_card_segments", args.run_id)
        df = read_lake(spark, silver_path)
        prepared_df, inputs = prepare(df)
        assembled = VectorAssembler(inputCols=inputs, outputCol="features_unscaled", handleInvalid="error").transform(prepared_df)
        scaler = StandardScaler(inputCol="features_unscaled", outputCol="features", withMean=True, withStd=True).fit(assembled)
        scaled = scaler.transform(assembled).select("cust_id", *FEATURES, "features").persist(StorageLevel.MEMORY_AND_DISK)
        n = scaled.count()
        if n < 3:
            raise ValueError("Se requieren al menos 3 clientes para comparar K")
        evaluator = ClusteringEvaluator(featuresCol="features", predictionCol="cluster", metricName="silhouette", distanceMeasure="squaredEuclidean")
        results = []
        best = None
        for k in range(args.k_min, min(args.k_max, n - 1) + 1):
            model = KMeans(k=k, seed=args.seed, maxIter=40, featuresCol="features", predictionCol="cluster").fit(scaled)
            predictions = model.transform(scaled)
            sizes = {int(r["cluster"]): int(r["count"]) for r in predictions.groupBy("cluster").count().collect()}
            if len(sizes) < k:
                print(f"K={k}: clusters vacíos, descartado")
                continue
            score = float(evaluator.evaluate(predictions))
            inertia = float(model.summary.trainingCost)
            entry = {"k": k, "silhouette": score, "inertia": inertia, "cluster_sizes": sizes}
            results.append(entry)
            print(json.dumps(entry, ensure_ascii=False))
            if best is None or score > best["silhouette"]:
                best = entry
        if best is None:
            raise RuntimeError("Ningún K produjo clusters válidos")
        k = best["k"]
        final_model = KMeans(k=k, seed=args.seed, maxIter=40, featuresCol="features", predictionCol="cluster").fit(scaled)
        output = (final_model.transform(scaled)
            .drop("features")
            .withColumn("cluster", F.col("cluster").cast(IntegerType()))
            .withColumn("model_version", F.lit("kmeans-v1"))
            .withColumn("processed_at", F.current_timestamp()))
        write_lake(output, gold_path)
        metrics = {"run_id": args.run_id, "source": silver_path, "destination": gold_path,
                   "customers": n, "selected_k": k, "selection_metric": "silhouette_squared_euclidean",
                   "features": FEATURES, "experiments": results,
                   "note": "Silhouette es validación interna; no prueba utilidad comercial ni estabilidad."}
        Path(args.metrics_dir).mkdir(parents=True, exist_ok=True)
        metrics_file = Path(args.metrics_dir) / f"kmeans_dataset2_{safe_id}.json"
        metrics_file.write_text(json.dumps(metrics, ensure_ascii=False, indent=2), encoding="utf-8")
        print(f"Gold: {gold_path}\nMétricas: {metrics_file}\nK seleccionado: {k}")
    finally:
        if scaled is not None:
            try: scaled.unpersist()
            except Exception: pass
        try: spark.stop()
        except Exception: pass


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("--run-id", required=True)
    parser.add_argument("--k-min", type=int, default=2)
    parser.add_argument("--k-max", type=int, default=8)
    parser.add_argument("--seed", type=int, default=42)
    parser.add_argument("--metrics-dir", default="/opt/airflow/logs/kmeans_metrics")
    args = parser.parse_args()
    if args.k_min < 2 or args.k_max < args.k_min:
        parser.error("Se requiere 2 <= k-min <= k-max")
    run(args)
