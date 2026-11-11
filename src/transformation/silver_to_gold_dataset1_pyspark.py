import argparse
import logging
import os
import sys
import time
from datetime import datetime
from pathlib import Path

from pyspark.sql import SparkSession, functions as F
from pyspark.sql.window import Window

ROOT_DIR = Path(__file__).resolve().parents[2]
sys.path.insert(0, str(ROOT_DIR))
from src.validation.validate_gold_contract import validate_gold_contract  # noqa: E402
from src.common.spark import spark_session, read_lake, write_lake
from src.common.config import lake_path
from src.common.observability import record_metrics

LOG_DIR = Path(os.getenv("PIPELINE_LOG_DIR", str(ROOT_DIR / "logs" / "pipeline")))
LOG_DIR.mkdir(parents=True, exist_ok=True)
logging.basicConfig(
    filename=str(LOG_DIR / "silver_to_gold_dataset1.log"),
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(name)s: %(message)s",
)
logger = logging.getLogger("GoldDataset1Pipeline")
console = logging.StreamHandler()
console.setLevel(logging.INFO)
console.setFormatter(logging.Formatter("%(asctime)s [%(levelname)s] %(message)s"))
if not any(isinstance(h, logging.StreamHandler) for h in logger.handlers):
    logger.addHandler(console)


def get_spark_session():
    return spark_session("Gold_Dataset1")


def process_gold_dataset1(run_id):
    start = time.time()
    spark = get_spark_session()
    silver_path = lake_path("silver", "personal_transactions", run_id)
    monthly_path = lake_path("gold", "personal_transactions", run_id)
    category_path = lake_path("gold", "personal_transactions_category_spending", run_id)
    monthly_contract = ROOT_DIR / "schemas/gold/gold_monthly_cashflow.json"
    category_contract = ROOT_DIR / "schemas/gold/gold_category_spending.json"
    status = "FAILED"
    total_rows = 0

    try:
        logger.info("Dataset1 Gold: lectura de %s", silver_path)
        df_silver = read_lake(spark, silver_path)
        for name in df_silver.columns:
            df_silver = df_silver.withColumnRenamed(name, name.lower())

        required = {"date", "transaction_description", "category", "amount", "type"}
        missing = sorted(required - set(df_silver.columns))
        if missing:
            raise ValueError(f"Faltan columnas Silver requeridas: {missing}")

        # Estandarizar tipos y categorías; los importes se agregan como valores absolutos.
        df_clean = (
            df_silver
            .withColumn("date", F.to_date(F.col("date")))
            .withColumn("transaction_description", F.upper(F.trim(F.col("transaction_description"))))
            .withColumn("category", F.initcap(F.trim(F.col("category"))))
            .withColumn("type", F.upper(F.trim(F.col("type"))))
            .withColumn("amount", F.col("amount").cast("double"))
        )

        df_filtered = (
            df_clean.filter(
                F.col("date").isNotNull()
                & F.col("amount").isNotNull()
                & F.col("type").isin("INCOME", "EXPENSE")
                & F.col("category").isNotNull()
                & (F.trim(F.col("category")) != "")
            )
            .withColumn(
                "period_month",
                F.to_date(F.concat(F.date_format(F.col("date"), "yyyy-MM"), F.lit("-01"))),
            )
        )

        # GOLD 1: una fila por mes.
        df_monthly = df_filtered.groupBy("period_month").agg(
            F.count(F.lit(1)).cast("long").alias("transaction_count"),
            F.sum(F.when(F.col("type") == "INCOME", 1).otherwise(0)).cast("long").alias("income_transaction_count"),
            F.sum(F.when(F.col("type") == "EXPENSE", 1).otherwise(0)).cast("long").alias("expense_transaction_count"),
            F.sum(F.when(F.col("type") == "INCOME", F.abs(F.col("amount"))).otherwise(F.lit(0.0))).cast("decimal(18,2)").alias("total_income"),
            F.sum(F.when(F.col("type") == "EXPENSE", F.abs(F.col("amount"))).otherwise(F.lit(0.0))).cast("decimal(18,2)").alias("total_expenses"),
        )
        df_monthly = (
            df_monthly
            .withColumn("net_cash_flow", (F.col("total_income") - F.col("total_expenses")).cast("decimal(18,2)"))
            .withColumn(
                "income_to_expense_ratio",
                F.when(F.col("total_expenses") > 0,
                       (F.col("total_income").cast("double") / F.col("total_expenses").cast("double")).cast("decimal(12,6)"))
                .otherwise(F.lit(None).cast("decimal(12,6)")),
            )
            .withColumn(
                "estimated_net_cash_flow_rate_pct",
                F.when(F.col("total_income") > 0,
                       (F.col("net_cash_flow").cast("double") / F.col("total_income").cast("double") * 100).cast("decimal(9,4)"))
                .otherwise(F.lit(None).cast("decimal(9,4)")),
            )
            .withColumn(
                "financial_health_score",
                F.when(F.col("total_expenses") == 0, F.lit("Sin gastos registrados"))
                 .when(F.col("income_to_expense_ratio") >= 1.5, F.lit("Excelente"))
                 .when(F.col("income_to_expense_ratio") >= 1.1, F.lit("Buena"))
                 .when(F.col("income_to_expense_ratio") >= 1.0, F.lit("Regular"))
                 .otherwise(F.lit("En Riesgo")),
            )
            .select(
                "period_month", "transaction_count", "income_transaction_count",
                "expense_transaction_count", "total_income", "total_expenses",
                "net_cash_flow", "income_to_expense_ratio",
                "estimated_net_cash_flow_rate_pct", "financial_health_score",
            )
        )

        # GOLD 2: gasto mensual por categoría. Solo transacciones EXPENSE.
        expense_rows = df_filtered.filter(F.col("type") == "EXPENSE")
        df_category = expense_rows.groupBy("period_month", "category").agg(
            F.count(F.lit(1)).cast("long").alias("expense_transaction_count"),
            F.sum(F.abs(F.col("amount"))).cast("decimal(18,2)").alias("expense_total"),
        )
        month_window = Window.partitionBy("period_month")
        df_category = (
            df_category
            .withColumn("_month_expense_total", F.sum(F.col("expense_total")).over(month_window))
            .withColumn(
                "expense_share_pct",
                F.when(F.col("_month_expense_total") > 0,
                       (F.col("expense_total").cast("double") / F.col("_month_expense_total").cast("double") * 100).cast("decimal(9,4)"))
                .otherwise(F.lit(None).cast("decimal(9,4)")),
            )
            .drop("_month_expense_total")
            .select("period_month", "category", "expense_transaction_count", "expense_total", "expense_share_pct")
        )

        # Validar ambos DataFrames antes de escribir: el contrato bloquea la publicación si falla.
        monthly_validation = validate_gold_contract(df_monthly, monthly_contract)
        category_validation = validate_gold_contract(df_category, category_contract)
        logger.info("Contrato mensual: %s", monthly_validation)
        logger.info("Contrato por categoría: %s", category_validation)

        monthly_count = df_monthly.count()
        category_count = df_category.count()
        write_lake(df_monthly, monthly_path)
        write_lake(df_category, category_path)
        total_rows = monthly_count + category_count
        status = "SUCCESS"
        logger.info("Gold Dataset1 escrito. Mensual=%s, categoría=%s", monthly_count, category_count)
        return monthly_count
    except Exception:
        logger.exception("Falló la construcción/validación de Gold Dataset1")
        raise
    finally:
        elapsed = round(time.time() - start, 2)
        record_metrics(records_written=total_rows, output_status=status)
        logger.info("Dataset1 Gold finalizado: status=%s duración=%ss", status, elapsed)
        spark.stop()


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Silver a Gold - Dataset 1")
    parser.add_argument("--run_id", required=True, help="Airflow Run ID")
    args = parser.parse_args()
    process_gold_dataset1(args.run_id)
