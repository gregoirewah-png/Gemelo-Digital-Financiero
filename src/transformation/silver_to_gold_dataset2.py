import argparse
from datetime import datetime
import logging
import os
from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.ml.feature import VectorAssembler, StandardScaler
from pyspark.ml.clustering import KMeans
import traceback
import psycopg2

# 1. Configurar directorio y archivo de logs local
log_dir = "/opt/airflow/logs/custom_pipeline_logs"
os.makedirs(log_dir, exist_ok=True)
log_file_path = os.path.join(log_dir, "silver_to_gold_credit_card.log")

logging.basicConfig(
    filename=log_file_path,
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(name)s: %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S"
)
logger = logging.getLogger("GoldCreditCardPipeline")

console_handler = logging.StreamHandler()
console_handler.setLevel(logging.INFO)
console_handler.setFormatter(logging.Formatter("%(asctime)s [%(levelname)s] %(message)s"))
logger.addHandler(console_handler)

def get_spark_session():
    spark = SparkSession.builder \
        .appName("SilverToGold_Dataset2_CreditCard") \
        .config("spark.jars.packages", "org.postgresql:postgresql:42.6.0,org.apache.hadoop:hadoop-aws:3.3.4,com.amazonaws:aws-java-sdk-bundle:1.12.262") \
        .config("spark.hadoop.fs.s3a.endpoint", "http://gemelo-minio:9000") \
        .config("spark.hadoop.fs.s3a.access.key", "minioadmin") \
        .config("spark.hadoop.fs.s3a.secret.key", "minioadmin123") \
        .config("spark.hadoop.fs.s3a.path.style.access", "true") \
        .config("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem") \
        .config("spark.hadoop.fs.s3a.aws.credentials.provider", "org.apache.hadoop.fs.s3a.SimpleAWSCredentialsProvider") \
        .config("spark.hadoop.fs.s3a.fast.upload", "false") \
        .config("spark.sql.shuffle.partitions", "1") \
        .config("spark.default.parallelism", "1") \
        .config("spark.driver.memory", "2g") \
        .config("spark.driver.maxResultSize", "1g") \
        .config("spark.memory.fraction", "0.6") \
        .getOrCreate()

    hadoop_conf = spark.sparkContext._jsc.hadoopConfiguration()
    hadoop_conf.set("fs.s3a.connection.timeout", "600000")
    hadoop_conf.set("fs.s3a.connection.establish.timeout", "60000")
    hadoop_conf.set("fs.s3a.threads.keepalivetime", "60000")
    hadoop_conf.set("fs.s3a.multipart.purge.age", "86400000")

    return spark

def log_execution_to_postgres(run_id, status="SUCCESS", dataset="Credit Card Gold Segmentation", records_count=0):
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

def process_gold_credit_card(run_id):
    spark = get_spark_session()
    
    silver_path = "s3a://silver/credit_card_balances/"
    gold_path = f"s3a://gold/credit_card_balances/run_id={run_id}"
    
    logger.info(f"--- Iniciando construcción de Capa Gold - Credit Card Segmentation (Run ID: {run_id}) ---")
    logger.info(f"Leyendo datos Silver desde: {silver_path}")
    
    df_silver = spark.read.parquet(silver_path)
    
    # Solución: Convertir los nombres de todas las columnas a MAYÚSCULAS para evitar conflictos
    for col_name in df_silver.columns:
        df_silver = df_silver.withColumnRenamed(col_name, col_name.upper())

    # Excluir identificadores únicos si están presentes
    columns_to_drop = [col for col in ["CUST_ID", "CUSTOMER_ID"] if col in df_silver.columns]
    if columns_to_drop:
        logger.info(f"Descartando columnas de identificación: {columns_to_drop}")
        df_silver = df_silver.drop(*columns_to_drop)
    
    # Variables numéricas reales del dataset (en mayúsculas)
    feature_cols = [
        'BALANCE', 'BALANCE_FREQUENCY', 'PURCHASES', 'ONEOFF_PURCHASES', 
        'INSTALLMENTS_PURCHASES', 'CASH_ADVANCE', 'PURCHASES_FREQUENCY', 
        'ONEOFF_PURCHASES_FREQUENCY', 'PURCHASES_INSTALLMENTS_FREQUENCY', 
        'CASH_ADVANCE_FREQUENCY', 'CASH_ADVANCE_TRX', 'PURCHASES_TRX', 
        'CREDIT_LIMIT', 'PAYMENTS', 'MINIMUM_PAYMENTS', 'PRC_FULL_PAYMENT', 'TENURE'
    ]
    
    # Asegurar que solo se usen las columnas que existan y convertirlas a tipo Double
    valid_features = [col for col in feature_cols if col in df_silver.columns]
    logger.info(f"Columnas válidas detectadas para el modelo: {valid_features}")
    
    for col_name in valid_features:
        df_silver = df_silver.withColumn(col_name, F.col(col_name).cast("double"))
    
    # Manejo de valores nulos
    logger.info("Rellenando valores nulos en el DataFrame...")
    df_clean = df_silver.na.fill(0, subset=valid_features)
    
    # 1. Ensamblar características en un vector para MLlib
    assembler = VectorAssembler(inputCols=valid_features, outputCol="features_unscaled")
    df_assembled = assembler.transform(df_clean)
    
    # 2. Escalar características (StandardScaler)
    scaler = StandardScaler(inputCol="features_unscaled", outputCol="features", withMean=True, withStd=True)
    scaler_model = scaler.fit(df_assembled)
    df_scaled = scaler_model.transform(df_assembled).cache()  # <--- Mantener en caché
    _ = df_scaled.count()                                     # <--- Forzar materialización en RAM

    # 3. Aplicar modelo K-Means para segmentación de clientes de forma segura
    logger.info("Entrenando modelo K-Means (4 clusters)...")
    kmeans = KMeans(featuresCol="features", predictionCol="cluster", k=4, seed=42)
    kmeans_model = kmeans.fit(df_scaled)
    df_gold = kmeans_model.transform(df_scaled)
    
    output_cols = valid_features + ["cluster"]
    df_gold_final = df_gold.select(*output_cols)
    
    logger.info("Escribiendo resultados calculados en la capa Gold de MinIO...")
    df_gold_final.coalesce(1).write.mode("overwrite").parquet(gold_path)
    
    records_count = df_gold_final.count()
    log_execution_to_postgres(run_id, status="SUCCESS", dataset="Credit Card Gold Segmentation", records_count=records_count)
    
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
        total_records = process_gold_credit_card(args.run_id)
        logger.info("Proceso de Capa Gold Credit Card completado exitosamente.")
    except Exception as e:
        execution_status = "FALLIDO (FAILED)"
        error_detalle = str(e)
        logger.error(f"Error crítico en Capa Gold Credit Card: {error_detalle}")
        logger.error(traceback.format_exc())
        log_execution_to_postgres(args.run_id, status="FAILED", dataset="Credit Card Gold Segmentation", records_count=0)
        raise e
    finally:
        print("\n" + "="*60)
        print("    REPORTE DE EJECUCIÓN - CAPA GOLD (CREDIT CARD SEGMENTATION)")
        print("="*60)
        print(f" Run ID          : {args.run_id}")
        print(f" Estado          : {execution_status}")
        if execution_status == "FALLIDO (FAILED)":
            print(f" Motivo del error: {error_detalle}")
        else:
            print(f" Registros Gold  : {total_records}")
        print(f" Destino MinIO   : s3a://gold/credit_card_balances/")
        print(f" Timestamp fin   : {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
        print("="*60 + "\n")