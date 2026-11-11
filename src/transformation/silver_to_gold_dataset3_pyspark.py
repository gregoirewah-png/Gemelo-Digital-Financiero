import argparse
import logging
import os
import sys
import time
from datetime import datetime
from pathlib import Path

from pyspark.sql import SparkSession, functions as F

ROOT_DIR = Path(__file__).resolve().parents[2]
sys.path.insert(0, str(ROOT_DIR))
from src.validation.validate_gold_contract import validate_gold_contract  # noqa: E402
from src.common.spark import spark_session, read_lake, write_lake
from src.common.config import lake_path
from src.common.observability import record_metrics

LOG_DIR = Path(os.getenv("PIPELINE_LOG_DIR", str(ROOT_DIR / "logs" / "pipeline")))
LOG_DIR.mkdir(parents=True, exist_ok=True)
logging.basicConfig(
    filename=str(LOG_DIR / "silver_to_gold_dataset3.log"),
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(name)s: %(message)s",
)
logger = logging.getLogger("GoldFinancialProfilePipeline")
console = logging.StreamHandler()
console.setLevel(logging.INFO)
console.setFormatter(logging.Formatter("%(asctime)s [%(levelname)s] %(message)s"))
if not any(isinstance(h, logging.StreamHandler) for h in logger.handlers):
    logger.addHandler(console)


def get_spark_session():
    return spark_session("Gold_Dataset3")


def process_gold_financial_profile(run_id):
    start = time.time()
    spark = get_spark_session()
    silver_path = lake_path("silver", "financial_profile", run_id)
    gold_path = lake_path("gold", "financial_profile", run_id)
    contract_path = ROOT_DIR / "schemas/gold/gold_financial_profile.json"
    status = "FAILED"
    records_count = 0

    try:
        df_silver = read_lake(spark, silver_path)
        required = {
            "user_id", "record_date", "age", "monthly_income_usd", "monthly_expenses_usd",
            "savings_usd", "credit_score", "has_loan", "loan_amount_usd",
            "monthly_emi_usd", "debt_to_income_ratio",
        }
        missing = sorted(required - set(df_silver.columns))
        if missing:
            raise ValueError(f"Faltan columnas Silver requeridas: {missing}")

        # Normalizar explícitamente los tipos y el booleano has_loan.
        has_loan_text = F.upper(F.trim(F.col("has_loan").cast("string")))
        df_gold = df_silver.select(
            F.col("user_id").cast("string").alias("user_id"),
            F.to_date(F.col("record_date")).alias("record_date"),
            F.col("age").cast("int").alias("age"),
            F.col("monthly_income_usd").cast("decimal(18,2)").alias("monthly_income_usd"),
            F.col("monthly_expenses_usd").cast("decimal(18,2)").alias("monthly_expenses_usd"),
            F.col("savings_usd").cast("decimal(18,2)").alias("savings_usd"),
            F.col("credit_score").cast("int").alias("credit_score"),
            F.when(has_loan_text.isin("TRUE", "YES", "1"), F.lit(True))
             .when(has_loan_text.isin("FALSE", "NO", "0"), F.lit(False))
             .otherwise(F.lit(None).cast("boolean")).alias("has_loan"),
            F.col("loan_amount_usd").cast("decimal(18,2)").alias("loan_amount_usd"),
            F.col("monthly_emi_usd").cast("decimal(18,2)").alias("monthly_emi_usd"),
            F.col("debt_to_income_ratio").cast("decimal(12,6)").alias("debt_to_income_ratio"),
        )

        # Métrica A: capacidad mensual después de gastos y EMI reportada.
        df_gold = df_gold.withColumn(
            "savings_capacity",
            (
                F.col("monthly_income_usd")
                - F.col("monthly_expenses_usd")
                - F.coalesce(F.col("monthly_emi_usd"), F.lit(0).cast("decimal(18,2)"))
            ).cast("decimal(18,2)"),
        )

        # Métrica B: meses de gastos cubiertos por los ahorros reportados.
        df_gold = df_gold.withColumn(
            "emergency_fund_months",
            F.when(
                F.col("monthly_expenses_usd") > 0,
                (F.col("savings_usd") / F.col("monthly_expenses_usd")).cast("decimal(12,1)"),
            ).otherwise(F.lit(0).cast("decimal(12,1)")),
        )

        # Clasificación heurística del proyecto; no equivale a una decisión crediticia real.
        df_gold = df_gold.withColumn(
            "nivel_score_category",
            F.when(F.col("credit_score") >= 750, F.lit("Excelente 🟢"))
             .when(F.col("credit_score") >= 670, F.lit("Bueno 🟡"))
             .when(F.col("credit_score") >= 580, F.lit("Regular 🟠"))
             .otherwise(F.lit("Bajo 🔴")),
        ).withColumn(
            "nivel_score_meaning",
            F.when(F.col("credit_score") >= 750, F.lit("Score alto según los umbrales heurísticos del prototipo."))
             .when(F.col("credit_score") >= 670, F.lit("Score favorable según los umbrales heurísticos del prototipo."))
             .when(F.col("credit_score") >= 580, F.lit("Score intermedio según los umbrales heurísticos del prototipo."))
             .otherwise(F.lit("Score bajo según los umbrales heurísticos del prototipo.")),
        )

        df_gold = df_gold.withColumn(
            "eligible_for_credit",
            F.when(
                (F.col("credit_score") >= 580) & (F.col("savings_capacity") > 100),
                F.lit(True),
            ).otherwise(F.lit(False)),
        ).withColumn(
            "suggested_max_credit_limit",
            F.when(
                F.col("eligible_for_credit"),
                (F.col("savings_capacity") * F.lit(12.0) * F.lit(2.5)).cast("decimal(18,2)"),
            ).otherwise(F.lit(0).cast("decimal(18,2)")),
        ).withColumn("processed_at", F.current_timestamp())

        # Orden de columnas estable y explícito.
        df_gold = df_gold.select(
            "user_id", "record_date", "age", "monthly_income_usd", "monthly_expenses_usd",
            "savings_usd", "credit_score", "has_loan", "loan_amount_usd", "monthly_emi_usd",
            "debt_to_income_ratio", "savings_capacity", "emergency_fund_months",
            "nivel_score_category", "nivel_score_meaning", "eligible_for_credit",
            "suggested_max_credit_limit", "processed_at",
        )

        validation = validate_gold_contract(df_gold, contract_path)
        logger.info("Resultado de contrato Gold Dataset3: %s", validation)

        records_count = df_gold.count()
        write_lake(df_gold, gold_path)
        status = "SUCCESS"
        logger.info("Gold Dataset3 escrito en %s (%s registros)", gold_path, records_count)
        return records_count
    except Exception:
        logger.exception("Falló la construcción/validación de Gold Dataset3")
        raise
    finally:
        record_metrics(records_written=records_count, output_status=status)
        logger.info("Dataset3 Gold finalizado: status=%s duración=%ss", status, round(time.time() - start, 2))
        spark.stop()


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Silver a Gold - Dataset 3")
    parser.add_argument("--run_id", required=True, help="Airflow Run ID")
    args = parser.parse_args()
    process_gold_financial_profile(args.run_id)
