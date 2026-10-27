import argparse
from datetime import datetime
import logging
import os
from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.window import Window
import traceback
import psycopg2

# 1. Configurar directorio y archivo de logs local
log_dir = "/opt/airflow/logs/custom_pipeline_logs"
os.makedirs(log_dir, exist_ok=True)
log_file_path = os.path.join(log_dir, "silver_to_gold_dataset1.log")

logging.basicConfig(
    filename=log_file_path,
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(name)s: %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S"
)
logger = logging.getLogger("GoldDataset1Pipeline")

console_handler = logging.StreamHandler()
console_handler.setLevel(logging.INFO)
console_handler.setFormatter(logging.Formatter("%(asctime)s [%(levelname)s] %(message)s"))
logger.addHandler(console_handler)

def get_spark_session():
    spark = SparkSession.builder \
        .appName("SilverToGold_Dataset1_Con_MinIO") \
        .config("spark.jars.packages", "org.postgresql:postgresql:42.6.0,org.apache.hadoop:hadoop-aws:3.3.4,com.amazonaws:aws-java-sdk-bundle:1.12.262") \
        .config("spark.hadoop.fs.s3a.endpoint", "http://gemelo-minio:9000") \
        .config("spark.hadoop.fs.s3a.access.key", "minioadmin") \
        .config("spark.hadoop.fs.s3a.secret.key", "minioadmin123") \
        .config("spark.hadoop.fs.s3a.path.style.access", "true") \
        .config("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem") \
        .config("spark.hadoop.fs.s3a.aws.credentials.provider", "org.apache.hadoop.fs.s3a.SimpleAWSCredentialsProvider") \
        .config("spark.hadoop.fs.s3a.threads.keepalivetime", "60") \
        .config("spark.hadoop.fs.s3a.connection.establish.timeout", "30000") \
        .config("spark.hadoop.fs.s3a.connection.timeout", "60000") \
        .config("spark.hadoop.fs.s3a.attempts.maximum", "3") \
        .config("spark.hadoop.fs.s3a.multipart.purge.age", "24") \
        .config("spark.hadoop.fs.s3a.connection.maximum", "100") \
        .config("spark.sql.parquet.writeLegacyFormat", "true") \
        .config("spark.sql.ansi.enabled", "false") \
        .config("spark.sql.shuffle.partitions", "2") \
        .config("spark.driver.memory", "1g") \
        .getOrCreate()
    return spark

def log_execution_to_postgres(run_id, status="SUCCESS", dataset="Dataset 1 Gold", records_count=0):
    try:
        connection = psycopg2.connect(
            host="postgres",
            database="gemelo_db",
            user="admin",
            password="admin"
        )
        cursor = connection.cursor()
        
        cursor.execute("""
            CREATE TABLE IF NOT EXISTS pipeline_execution_logs (
                id SERIAL PRIMARY KEY,
                run_id VARCHAR(255),
                dataset_name VARCHAR(100),
                status VARCHAR(50),
                records_processed INT,
                created_at TIMESTAMP
            );
        """)
        
        cursor.execute(
            "INSERT INTO pipeline_execution_logs (run_id, dataset_name, status, records_processed, created_at) VALUES (%s, %s, %s, %s, %s)",
            (run_id, dataset, status, records_count, datetime.now())
        )
        
        connection.commit()
        cursor.close()
        connection.close()
        logger.info("Log de ejecución guardado exitosamente en PostgreSQL.")
    except Exception as e:
        logger.error(f"Error al guardar el log en Postgres: {e}")

def process_gold_dataset1(run_id):
    spark = get_spark_session()
    
    silver_path = "s3a://silver/personal_transactions/"
    gold_path = f"s3a://gold/personal_transactions/run_id={run_id}"
    
    logger.info(f"--- Iniciando construcción de Capa Gold - Dataset 1 (Run ID: {run_id}) ---")
    logger.info(f"Leyendo datos Silver desde: {silver_path}")
    
    df_silver = spark.read.parquet(silver_path)
    
    # Normalizar todos los nombres de columnas a MAYÚSCULAS para evitar conflictos de nombres
    for col_name in df_silver.columns:
        df_silver = df_silver.withColumnRenamed(col_name, col_name.upper())

    # Ahora las columnas se llamarán: 'DATE', 'TRANSACTION_DESCRIPTION', 'CATEGORY', 'AMOUNT', 'TYPE'
    logger.info(f"Columnas disponibles normalizadas: {df_silver.columns}")
    
    # 1. Limpieza y estandarización con nombres en mayúsculas
    df_clean = df_silver.withColumn(
        "DATE", F.to_date(F.col("DATE"), "yyyy-MM-dd")
    ).withColumn(
        "TRANSACTION_DESCRIPTION", F.upper(F.trim(F.col("TRANSACTION_DESCRIPTION")))
    ).withColumn(
        "CATEGORY", F.upper(F.trim(F.col("CATEGORY")))
    ).withColumn(
        "TYPE", F.upper(F.trim(F.col("TYPE")))
    ).withColumn(
        "AMOUNT", F.col("AMOUNT").cast("double")
    )
    
    # 2. Filtrar nulos críticos y extraer el mes (YYYY-MM)
    df_filtered = df_clean.filter(
        (F.col("DATE").isNotNull()) & 
        (F.col("AMOUNT").isNotNull()) & 
        (F.col("TYPE").isNotNull())
    ).withColumn(
        "month", F.date_format(F.col("DATE"), "yyyy-MM")
    )
    
    # 3. Agrupación detallada por mes, categoría, descripción y tipo
    df_agg = df_filtered.groupBy("month", "CATEGORY", "TRANSACTION_DESCRIPTION", "TYPE").agg(
        F.sum(F.when((F.col("TYPE").isin(["INCOME", "DEPOSIT"])) | (F.col("AMOUNT") > 0), F.col("AMOUNT")).otherwise(0)).alias("income"),
        F.sum(F.when((F.col("TYPE").isin(["EXPENSE", "WITHDRAWAL"])) | (F.col("AMOUNT") < 0), F.abs(F.col("AMOUNT"))).otherwise(0)).alias("expense")
    )
    
    df_gold_detail = df_agg.withColumn("net_balance", F.col("income") - F.col("expense"))
    
    # 4. Agregación a nivel mensual global para calcular salud financiera y ratio
    df_monthly = df_gold_detail.groupBy("month").agg(
        F.sum("income").alias("total_income"),
        F.sum("expense").alias("total_expense")
    )
    
    df_monthly = df_monthly.withColumn(
        "income_to_expense_ratio",
        F.when(F.col("total_expense") > 0, F.col("total_income") / F.col("total_expense")).otherwise(F.lit(999.0))
    )
    
    # 5. Reglas de negocio para la Calificación de Salud Financiera
    df_monthly = df_monthly.withColumn(
        "financial_health_score",
        F.when(F.col("income_to_expense_ratio") >= 1.5, F.lit("Excelente"))
         .when((F.col("income_to_expense_ratio") >= 1.1) & (F.col("income_to_expense_ratio") < 1.5), F.lit("Buena"))
         .when((F.col("income_to_expense_ratio") >= 1.0) & (F.col("income_to_expense_ratio") < 1.1), F.lit("Regular"))
         .otherwise(F.lit("En Riesgo"))
    )
    
    logger.info("Escribiendo resultados calculados en la capa Gold de MinIO...")
    df_monthly.write.mode("overwrite").parquet(gold_path)
    
    records_count = df_monthly.count()
    log_execution_to_postgres(run_id, status="SUCCESS", dataset="Dataset 1 Gold", records_count=records_count)
    
    spark.stop()
    return records_count

if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("--run_id", required=True, help="Airflow Run ID")
    args = parser.parse_args()
    
    execution_status = "EXITOSO (SUCCESS)"
    error_detalle = ""
    total_records = 0
    
    try:
        total_records = process_gold_dataset1(args.run_id)
        logger.info("Proceso de Capa Gold Dataset 1 completado exitosamente.")
    except Exception as e:
        execution_status = "FALLIDO (FAILED)"
        error_detalle = str(e)
        logger.error(f"Error crítico en Capa Gold Dataset 1: {error_detalle}")
        logger.error(traceback.format_exc())
        log_execution_to_postgres(args.run_id, status="FAILED", dataset="Dataset 1 Gold", records_count=0)
        raise e
    finally:
        print("\n" + "="*60)
        print("       REPORTE DE EJECUCIÓN - CAPA GOLD (DATASET 1)")
        print("="*60)
        print(f" Run ID          : {args.run_id}")
        print(f" Estado          : {execution_status}")
        if execution_status == "FALLIDO (FAILED)":
            print(f" Motivo del error: {error_detalle}")
        else:
            print(f" Registros Gold  : {total_records}")
        print(f" Destino MinIO   : s3a://gold/personal_transactions/run_id={args.run_id}")
        print(f" Timestamp fin   : {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
        print("="*60 + "\n")