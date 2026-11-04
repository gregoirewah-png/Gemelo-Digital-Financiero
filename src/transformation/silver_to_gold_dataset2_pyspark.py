import argparse
import logging
import os
import sys
import time
from datetime import datetime
from pathlib import Path

import psycopg2
from pyspark.sql import SparkSession, functions as F
from pyspark.ml.feature import VectorAssembler, StandardScaler
from pyspark.ml.clustering import KMeans

ROOT_DIR = Path(__file__).resolve().parents[2]
sys.path.insert(0, str(ROOT_DIR))
from src.validation.validate_gold_contract import validate_gold_contract  # noqa: E402

LOG_DIR = Path(os.getenv("PIPELINE_LOG_DIR", "/opt/airflow/logs/custom_pipeline_logs"))
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
    return (
        SparkSession.builder.appName("SilverToGold_Dataset2_CreditCard")
        .config(
            "spark.jars.packages",
            "org.postgresql:postgresql:42.6.0,org.apache.hadoop:hadoop-aws:3.3.4,com.amazonaws:aws-java-sdk-bundle:1.12.262",
        )
        .config("spark.hadoop.fs.s3a.endpoint", os.getenv("S3_ENDPOINT", "http://gemelo-minio:9000"))
        .config("spark.hadoop.fs.s3a.access.key", os.getenv("MINIO_ACCESS_KEY", "minioadmin"))
        .config("spark.hadoop.fs.s3a.secret.key", os.getenv("MINIO_SECRET_KEY", "minioadmin123"))
        .config("spark.hadoop.fs.s3a.path.style.access", "true")
        .config("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem")
        .config("spark.hadoop.fs.s3a.aws.credentials.provider", "org.apache.hadoop.fs.s3a.SimpleAWSCredentialsProvider")
        .config("spark.sql.shuffle.partitions", "1")
        .config("spark.default.parallelism", "1")
        .config("spark.driver.memory", "2g")
        .config("spark.hadoop.fs.s3a.connection.timeout", "600000")
        .config("spark.hadoop.fs.s3a.connection.establish.timeout", "600000")
        .config("spark.hadoop.fs.s3a.threads.keepalivetime", "60")
        .config("spark.hadoop.fs.s3a.multipart.purge.age", "86400")
        .config("spark.driver.maxResultSize", "1g")
        .getOrCreate()
    )
    hadoop_conf = spark.sparkContext._jsc.hadoopConfiguration()
    hadoop_conf.set("fs.s3a.connection.timeout", "600000")
    hadoop_conf.set("fs.s3a.connection.establish.timeout", "600000")
    hadoop_conf.set("fs.s3a.threads.keepalivetime", "60")
    hadoop_conf.set("fs.s3a.multipart.purge.age", "86400")

def log_execution_to_postgres(run_id, status, records_count, dataset_name):
    connection = cursor = None
    try:
        connection = psycopg2.connect(
            host=os.getenv("POSTGRES_HOST", "postgres"),
            database=os.getenv("POSTGRES_DB", "gemelo_db"),
            user=os.getenv("POSTGRES_USER", "admin"),
            password=os.getenv("POSTGRES_PASSWORD", "admin"),
            port=os.getenv("POSTGRES_PORT", "5432"),
        )
        cursor = connection.cursor()
        cursor.execute(
            """CREATE TABLE IF NOT EXISTS pipeline_execution_logs (
                id SERIAL PRIMARY KEY, run_id VARCHAR(255), dataset_name VARCHAR(150),
                status VARCHAR(50), records_processed BIGINT, created_at TIMESTAMP
            )"""
        )
        cursor.execute(
            """INSERT INTO pipeline_execution_logs
               (run_id, dataset_name, status, records_processed, created_at)
               VALUES (%s, %s, %s, %s, %s)""",
            (run_id, dataset_name, status, records_count, datetime.now()),
        )
        connection.commit()
    except Exception:
        logger.exception("No se pudo registrar la ejecución en PostgreSQL")
    finally:
        if cursor:
            cursor.close()
        if connection:
            connection.close()


def process_gold_credit_card(run_id):
    start = time.time()
    spark = get_spark_session()
    safe_run_id = run_id.replace("+", "_").replace(":", "_").replace("/", "_").replace(" ", "_")
    silver_path = "s3a://silver/credit_card_balances/"
    gold_path = f"s3a://gold/credit_card_balances/run_id={safe_run_id}"
    contract_path = ROOT_DIR / "schemas/gold/gold_credit_card_summary.json"
    df_scaled = None
    status = "FAILED"
    records_count = 0

    try:
        df_silver = spark.read.parquet(silver_path)
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
        df_gold_final.coalesce(1).write.mode("overwrite").parquet(gold_path)
        status = "SUCCESS"
        logger.info("Gold Dataset2 escrito en %s (%s registros)", gold_path, records_count)
        return records_count
    except Exception:
        logger.exception("Falló la construcción/validación de Gold Dataset2")
        raise
    finally:
        if df_scaled is not None:
            df_scaled.unpersist()
        log_execution_to_postgres(run_id, status, records_count, "Credit Card Gold Segmentation")
        logger.info("Dataset2 Gold finalizado: status=%s duración=%ss", status, round(time.time() - start, 2))
        spark.stop()


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Silver a Gold - Dataset 2 (K-Means)")
    parser.add_argument("--run_id", required=True, help="Airflow Run ID")
    args = parser.parse_args()
    process_gold_credit_card(args.run_id)
