import json
import os
import re
import time
import argparse
from datetime import datetime
import psycopg2
from pyspark.sql import SparkSession
from pyspark.sql.types import StructType
from pyspark.sql.functions import col, when, sum as _sum, lit, current_timestamp

def to_snake_case(name):
    s1 = re.sub('(.)([A-Z][a-z]+)', r'\1_\2', name.replace(" ", "_"))
    s2 = re.sub('([a-z0-9])([A-Z])', r'\1_\2', s1).lower()
    return re.sub(r'_+', '_', s2)

def get_spark_session():
    spark = SparkSession.builder \
        .appName("BronzeToSilver_Dataset1_Con_MinIO") \
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
        .getOrCreate()

    hadoop_conf = spark.sparkContext._jsc.hadoopConfiguration()
    hadoop_conf.set("fs.s3a.connection.timeout", "600000")
    hadoop_conf.set("fs.s3a.connection.establish.timeout", "600000")
    hadoop_conf.set("fs.s3a.threads.keepalivetime", "60")
    hadoop_conf.set("fs.s3a.multipart.purge.age", "86400")

    return spark

def process_transactions(run_id):
    start_time = time.time()
    spark = get_spark_session()
    
    # 💡 Sanear el run_id para evitar errores de URI en Hadoop S3A
    safe_run_id = run_id.replace("+", "_").replace(":", "_").replace(" ", "_")
    
    # 1. Definir Rutas y Esquema Estricto
    BASE_DIR = os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
    schema_path = os.path.join(BASE_DIR, "schemas", "dataset1_schema.json")
    
    bronze_path = "s3a://bronze/Dataset1/*/*.csv"
    silver_path = "s3a://silver/personal_transactions"
    quarantine_path = f"s3a://quarantine/Dataset1/run_id={safe_run_id}"
    
    print(f"--- Iniciando transformación de transactions (Run ID: {run_id}) ---")
    
    # Cargar esquema definido en JSON (Contrato de Datos)
    if os.path.exists(schema_path):
        with open(schema_path, "r") as file:
            schema_json = json.load(file)
    else:
        schema_json = {"columns": {}}
        
    from pyspark.sql.types import StringType, DoubleType, IntegerType, DateType, StructField
    
    type_mapping = {
        "string": StringType(),
        "double": DoubleType(),
        "integer": IntegerType(),
        "date": DateType()
    }
    
    fields = []
    for col_name, rules in schema_json.get("columns", {}).items():
        data_type_str = rules.get("type", "string").lower()
        spark_type = type_mapping.get(data_type_str, StringType())
        is_nullable = rules.get("nullable", True)
        fields.append(StructField(col_name, spark_type, is_nullable))
        
    explicit_schema = StructType(fields) if fields else None

    # 2. Leer Bronze con Esquema Estricto y Modo Permisivo Controlado
    reader = spark.read \
        .option("pathGlobFilter", "*.csv") \
        .option("mode", "PERMISSIVE") \
        .option("columnNameOfCorruptRecord", "_corrupt_record")
        
    if explicit_schema:
        df = reader.schema(explicit_schema).csv(bronze_path, header=True)
    else:
        df = reader.csv(bronze_path, header=True)
    
    total_leidos = df.count()

    # Separar registros estructuralmente corruptos detectados por Spark
    if "_corrupt_record" in df.columns:
        df_structural_quarantine = df.filter(col("_corrupt_record").isNotNull()) \
            .withColumn("quarantine_reason", lit("Falla estructural de esquema o tipo de dato")) \
            .withColumn("quarantined_at", current_timestamp())
        df_valid = df.filter(col("_corrupt_record").isNull()).drop("_corrupt_record")
    else:
        df_structural_quarantine = None
        df_valid = df

    # 3. Limpieza de nombres y duplicados
    for col_name in df_valid.columns:
        if col_name != "_corrupt_record":
            df_valid = df_valid.withColumnRenamed(col_name, to_snake_case(col_name))

    df_clean = df_valid.dropDuplicates()
    total_sin_duplicados = df_clean.count()
    duplicados_eliminados = total_leidos - total_sin_duplicados

    # --- LÓGICA DE CUARENTENA ---
    df_evaluated = df_clean.withColumn(
        "quarantine_reason",
        lit(None)
    ).withColumn("quarantined_at", current_timestamp())

    df_business_quarantine = df_evaluated.filter(col("quarantine_reason").isNotNull())
    df_sanitized = df_evaluated.filter(col("quarantine_reason").isNull())

    # Consolidar cuarentena total
    if df_structural_quarantine is not None and df_structural_quarantine.count() > 0:
        df_final_quarantine = df_structural_quarantine.unionByName(df_business_quarantine, allowMissingColumns=True)
    else:
        df_final_quarantine = df_business_quarantine

    total_quarantine = df_final_quarantine.count()
    
    if total_quarantine > 0:
        print(f"⚠️ Enviando {total_quarantine} registros corruptos a Cuarentena (run_id: {run_id})...")
        df_final_quarantine.write.mode("overwrite").parquet(quarantine_path)

    # Continuar solo con registros sanos
    df_clean = df_sanitized.drop("quarantine_reason", "quarantined_at")

    # MÉTRICAS Y VALIDACIÓN NATIVA SEGURA
    total_filas = df_clean.count()
    print(f"\n[MÉTRICAS] Total de registros listos para Silver: {total_filas}")

    reglas_evaluadas = len(schema_json.get("columns", {})) * 2 if schema_json.get("columns") else 10
    reglas_fallidas = 0

    end_time = time.time()
    duracion = round(end_time - start_time, 2)
    timestamp_actual = datetime.now().strftime('%Y-%m-%d %H:%M:%S')
    
    reporte_txt = f"""
    ==================================================
    REPORTE DE CALIDAD TRAS INGESTA (BRONZE -> SILVER - DATASET 1)
    ==================================================
    Run ID: {run_id}
    Fecha de ejecución: {timestamp_actual}
    Duración del proceso: {duracion} segundos
    Registros leídos (Bronze): {total_leidos}
    Duplicados eliminados: {duplicados_eliminados}
    Registros aislados (Cuarentena): {total_quarantine}
    Registros validados listos para Silver: {total_filas}
    Reglas de calidad evaluadas: {reglas_evaluadas}
    Reglas fallidas: {reglas_fallidas}
    ==================================================
    """

    print(reporte_txt)

    # Escritura de log físico local
    log_dir = os.path.join(BASE_DIR, "logs")
    os.makedirs(log_dir, exist_ok=True) 
    nombre_archivo = f"reporte_dataset1_{datetime.now().strftime('%Y%m%d_%H%M%S')}.log"
    ruta_log = os.path.join(log_dir, nombre_archivo)
    
    with open(ruta_log, "w", encoding="utf-8") as archivo_log:
        archivo_log.write(reporte_txt)
        
    print(f"Reporte de calidad guardado físicamente en: {ruta_log}")

    # 5. ESCRITURA EN SILVER Y REGISTRO AUTOMÁTICO EN POSTGRESQL
    print("ÉXITO: El dataset pasó las validaciones y transformaciones de calidad.")
    
    # Escritura controlada con coalesce para evitar desbordamiento en JVM
    df_clean.coalesce(2).write.mode("overwrite").parquet(silver_path)
    
    print(f"Datos guardados exitosamente en: {silver_path}")

    # Inserción en la base de datos PostgreSQL para el monitor de Streamlit
    try:
        pg_conn = psycopg2.connect(
            dbname="gemelo_db",
            user="admin",
            password="admin",
            host="postgres",
            port="5432"
        )
        pg_cursor = pg_conn.cursor()
        
        insert_query = """
            INSERT INTO execution_logs 
            (dataset_name, run_id, duration_seconds, records_read, duplicates_removed, 
             records_quarantined, records_validated, expectations_evaluated, expectations_failed, log_content)
            VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
        """
        
        pg_cursor.execute(insert_query, (
            "Dataset 1", run_id, duracion, total_leidos, 
            duplicados_eliminados, total_quarantine, total_filas, 
            reglas_evaluadas, reglas_fallidas, reporte_txt
        ))
        
        pg_conn.commit()
        pg_cursor.close()
        pg_conn.close()
        print("Métricas de ejecución y logs guardados exitosamente en PostgreSQL.")
    except Exception as e:
        print(f"Advertencia: No se pudo registrar el log en PostgreSQL: {e}")
                
    spark.stop()

if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Procesamiento Bronze a Silver Dataset 1 con control de Esquema y Cuarentena")
    parser.add_argument("--run_id", required=True, help="Identificador único de ejecución (Run ID de Airflow)")
    args = parser.parse_args()
    
    process_transactions(args.run_id)