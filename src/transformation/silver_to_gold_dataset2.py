import argparse
from datetime import datetime
import logging
import os
from pyspark.sql import SparkSession
from pyspark.sql import functions as F
import traceback
import psycopg2

# 1. Configurar directorio y archivo de logs local
log_dir = "/opt/airflow/logs/custom_pipeline_logs"
os.makedirs(log_dir, exist_ok=True)
log_file_path = os.path.join(log_dir, "silver_to_gold_dataset2.log")

logging.basicConfig(
    filename=log_file_path,
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(name)s: %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S"
)
logger = logging.getLogger("GoldDataset2Pipeline")

console_handler = logging.StreamHandler()
console_handler.setLevel(logging.INFO)
console_handler.setFormatter(logging.Formatter("%(asctime)s [%(levelname)s] %(message)s"))
logger.addHandler(console_handler)

def get_spark_session():
    spark = SparkSession.builder \
        .appName("SilverToGold_Dataset2_Con_MinIO") \
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

def log_execution_to_postgres(run_id, status="SUCCESS", dataset="Dataset 2 Gold", records_count=0):
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

def process_gold_dataset2(run_id):
    spark = get_spark_session()
    
    silver_path = "s3a://silver/credit_card_balances/"
    gold_path = f"s3a://gold/credit_card_balances/run_id={run_id}"
    
    logger.info(f"--- Iniciando construcción de Capa Gold - Dataset 2 (Run ID: {run_id}) ---")
    logger.info(f"Leyendo datos Silver desde: {silver_path}")
    
    df_silver = spark.read.parquet(silver_path)
    
    # Descartar la columna cust_id si existe en el DataFrame
    columns_to_drop = [col for col in ["cust_id", "customer_id"] if col in df_silver.columns]
    if columns_to_drop:
        logger.info(f"Descartando columnas de identificación de cliente: {columns_to_drop}")
        df_silver = df_silver.drop(*columns_to_drop)
    
    # Procesamiento y agregaciones óptimas para la capa Gold
    # Agrupamos por dimensiones disponibles (ej. categoría, tipo de cuenta o estado) y calculamos métricas agregadas de comportamiento
    group_cols = [col for col in ["category", "transaction_type", "status"] if col in df_silver.columns]
    
    if group_cols:
        df_gold = df_silver.groupBy(*group_cols).agg(
            F.count("*").alias("total_transactions"),
            F.sum(F.col("amount")).alias("total_amount"),
            F.avg(F.col("amount")).alias("avg_amount"),
            F.max(F.col("amount")).alias("max_amount"),
            F.min(F.col("amount")).alias("min_amount")
        )
    else:
        # Fallback general si las columnas varían
        df_gold = df_silver.groupBy().agg(
            F.count("*").alias("total_transactions"),
            F.sum(F.col("amount")).alias("total_amount") if "amount" in df_silver.columns else F.lit(0).alias("total_amount")
        )
    
    logger.info("Escribiendo resultados calculados en la capa Gold de MinIO...")
    df_gold.write.mode("overwrite").parquet(gold_path)
    
    records_count = df_gold.count()
    log_execution_to_postgres(run_id, status="SUCCESS", dataset="Dataset 2 Gold", records_count=records_count)
    
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
        total_records = process_gold_dataset2(args.run_id)
        logger.info("Proceso de Capa Gold Dataset 2 completado exitosamente.")
    except Exception as e:
        execution_status = "FALLIDO (FAILED)"
        error_detalle = str(e)
        logger.error(f"Error crítico en Capa Gold Dataset 2: {error_detalle}")
        logger.error(traceback.format_exc())
        log_execution_to_postgres(args.run_id, status="FAILED", dataset="Dataset 2 Gold", records_count=0)
        raise e
    finally:
        print("\n" + "="*60)
        print("       REPORTE DE EJECUCIÓN - CAPA GOLD (DATASET 2)")
        print("="*60)
        print(f" Run ID          : {args.run_id}")
        print(f" Estado          : {execution_status}")
        if execution_status == "FALLIDO (FAILED)":
            print(f" Motivo del error: {error_detalle}")
        else:
            print(f" Registros Gold  : {total_records}")
        print(f" Destino MinIO   : s3a://gold/Dataset2/")
        print(f" Timestamp fin   : {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
        print("="*60 + "\n")