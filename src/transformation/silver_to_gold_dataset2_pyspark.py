import argparse
import logging
import os
import sys
import time
from datetime import datetime
from pathlib import Path

from pyspark.sql import SparkSession, functions as F
from pyspark.ml.feature import VectorAssembler, StandardScaler
from pyspark.ml.clustering import KMeans

ROOT_DIR = Path(__file__).resolve().parents[2]
sys.path.insert(0, str(ROOT_DIR))
from src.validation.validate_gold_contract import validate_gold_contract  # noqa: E402
from src.common.spark import spark_session, read_lake, write_lake
from src.common.config import lake_path
from src.common.observability import record_metrics

LOG_DIR = Path(os.getenv("PIPELINE_LOG_DIR", str(ROOT_DIR / "logs" / "pipeline")))
LOG_DIR.mkdir(parents=True, exist_ok=True)
logging.basicConfig(
    filename=str(LOG_DIR / "silver_to_gold_credit_card.log"),
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(name)s: %(message)s",
)
logger = logging.getLogger("GoldCreditCardPipeline")
console = logging.StreamHandler()
console.setLevel(logging.INFO)
console.setFormatter(logging.Formatter("%(asctime)s [%(levelname)s] %(message)s"))
if not any(isinstance(h, logging.StreamHandler) for h in logger.handlers):
    logger.addHandler(console)


FEATURE_COLS = [
    "BALANCE", "BALANCE_FREQUENCY", "PURCHASES", "ONEOFF_PURCHASES",
    "INSTALLMENTS_PURCHASES", "CASH_ADVANCE", "PURCHASES_FREQUENCY",
    "ONEOFF_PURCHASES_FREQUENCY", "PURCHASES_INSTALLMENTS_FREQUENCY",
    "CASH_ADVANCE_FREQUENCY", "CASH_ADVANCE_TRX", "PURCHASES_TRX",
    "CREDIT_LIMIT", "PAYMENTS", "MINIMUM_PAYMENTS", "PRC_FULL_PAYMENT", "TENURE",
]


def get_spark_session():
    return spark_session("Gold_Dataset2")


def process_gold_credit_card(run_id):
    start = time.time()
    spark = get_spark_session()
    silver_path = lake_path("silver", "credit_card_balances", run_id)
    gold_path = lake_path("gold", "credit_card_balances", run_id)
    contract_path = ROOT_DIR / "schemas/gold/gold_credit_card_summary.json"
    df_scaled = None
    status = "FAILED"
    records_count = 0

    try:
        df_silver = read_lake(spark, silver_path)
        for name in df_silver.columns:
            df_silver = df_silver.withColumnRenamed(name, name.upper())

        if "CUST_ID" not in df_silver.columns:
            raise ValueError("Silver Dataset2 debe conservar CUST_ID para la llave primaria Gold.")
        missing = sorted(set(FEATURE_COLS) - set(df_silver.columns))
        if missing:
            raise ValueError(f"Faltan variables requeridas para K-Means: {missing}")

        # Mantener el identificador fuera del vector de ML, pero conservarlo para Gold.
        df_model = df_silver.select("CUST_ID", *FEATURE_COLS)
        for feature in FEATURE_COLS:
            df_model = df_model.withColumn(feature, F.col(feature).cast("double"))
            df_model = df_model.withColumn(
                feature,
                F.when(F.col(feature).isNull() | F.isnan(F.col(feature)), F.lit(0.0))
                 .otherwise(F.col(feature)),
            )

        assembler = VectorAssembler(inputCols=FEATURE_COLS, outputCol="features_unscaled")
        df_assembled = assembler.transform(df_model)
        scaler = StandardScaler(
            inputCol="features_unscaled", outputCol="features", withMean=True, withStd=True
        )
        scaler_model = scaler.fit(df_assembled)
        df_scaled = scaler_model.transform(df_assembled).cache()
        df_scaled.count()  # Materializar el conjunto escalado.

        logger.info("Entrenando K-Means con k=4")
        kmeans_model = KMeans(featuresCol="features", predictionCol="cluster", k=4, seed=42).fit(df_scaled)
        clustered = kmeans_model.transform(df_scaled)

        # Tipos Gold explícitos: importes decimal, frecuencias decimal, contadores long.
        money_cols = [
            "BALANCE", "PURCHASES", "ONEOFF_PURCHASES", "INSTALLMENTS_PURCHASES",
            "CASH_ADVANCE", "CREDIT_LIMIT", "PAYMENTS", "MINIMUM_PAYMENTS",
        ]
        frequency_cols = [
            "BALANCE_FREQUENCY", "PURCHASES_FREQUENCY", "ONEOFF_PURCHASES_FREQUENCY",
            "PURCHASES_INSTALLMENTS_FREQUENCY", "CASH_ADVANCE_FREQUENCY", "PRC_FULL_PAYMENT",
        ]
        txn_cols = ["CASH_ADVANCE_TRX", "PURCHASES_TRX"]

        expressions = [F.col("CUST_ID").cast("string").alias("cust_id")]
        expressions += [F.col(c).cast("decimal(18,2)").alias(c.lower()) for c in money_cols]
        expressions += [F.col(c).cast("decimal(9,6)").alias(c.lower()) for c in frequency_cols]
        expressions += [F.col(c).cast("long").alias(c.lower()) for c in txn_cols]
        expressions += [F.col("TENURE").cast("int").alias("tenure")]
        expressions += [
            (F.col("CREDIT_LIMIT") - F.col("BALANCE")).cast("decimal(18,2)").alias("available_credit"),
            F.when(
                F.col("CREDIT_LIMIT") > 0,
                (F.col("BALANCE") / F.col("CREDIT_LIMIT") * 100).cast("decimal(9,4)"),
            ).otherwise(F.lit(None).cast("decimal(9,4)")).alias("credit_utilization_pct"),
            F.col("cluster").cast("int").alias("cluster"),
            F.current_timestamp().alias("processed_at"),
        ]
        df_gold_final = clustered.select(*expressions)

        validation = validate_gold_contract(df_gold_final, contract_path)
        logger.info("Resultado de contrato Gold Dataset2: %s", validation)

        records_count = df_gold_final.count()
        write_lake(df_gold_final, gold_path)
        status = "SUCCESS"
        logger.info("Gold Dataset2 escrito en %s (%s registros)", gold_path, records_count)
        return records_count
    except Exception:
        logger.exception("Falló la construcción/validación de Gold Dataset2")
        raise
    finally:
        if df_scaled is not None:
            df_scaled.unpersist()
        record_metrics(records_written=records_count, output_status=status)
        logger.info("Dataset2 Gold finalizado: status=%s duración=%ss", status, round(time.time() - start, 2))
        spark.stop()


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Silver a Gold - Dataset 2 (K-Means)")
    parser.add_argument("--run_id", required=True, help="Airflow Run ID")
    args = parser.parse_args()
    process_gold_credit_card(args.run_id)
