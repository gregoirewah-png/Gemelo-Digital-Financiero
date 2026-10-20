import json
import os
import time
import re
import argparse
import psycopg2
from datetime import datetime
from pyspark.sql import SparkSession
from pyspark.sql.types import StructType
from pyspark.sql.functions import col, when, to_date, trim, initcap, coalesce, lit, current_timestamp
from pyspark.sql.functions import sum as _sum
from great_expectations.dataset.sparkdf_dataset import SparkDFDataset

def get_spark_session():
    spark = SparkSession.builder \
        .appName("BronzeToSilver_Con_MinIO") \
        .config("spark.jars.packages", "org.apache.hadoop:hadoop-aws:3.3.4,com.amazonaws:aws-java-sdk-bundle:1.12.262") \
        .config("spark.hadoop.fs.s3a.endpoint", "http://gemelo-minio:9000") \
        .config("spark.hadoop.fs.s3a.access.key", "minioadmin") \
        .config("spark.hadoop.fs.s3a.secret.key", "minioadmin123") \
        .config("spark.hadoop.fs.s3a.path.style.access", "true") \
        .config("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem") \
        .config("spark.hadoop.fs.s3a.aws.credentials.provider", "org.apache.hadoop.fs.s3a.SimpleAWSCredentialsProvider") \
        .config("spark.sql.parquet.writeLegacyFormat", "true") \
        .config("spark.sql.ansi.enabled", "false") \
        .config("spark.sql.shuffle.partitions", "2") \
        .config("spark.driver.memory", "1g") \
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
    
    # 1. Definir Rutas y Esquema Estricto
    BASE_DIR = os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
    schema_path = os.path.join(BASE_DIR, "schemas", "dataset1_schema.json")

    bronze_path = "s3a://bronze/Dataset1/*/*.csv"
    silver_path = "s3a://silver/personal_transactions"
    # Ruta de cuarentena acotada por run_id para garantizar idempotencia en reintentos
    quarantine_path = f"s3a://quarantine/Dataset1/run_id={run_id}"
    
    print(f"--- Iniciando transformación (Run ID: {run_id}) ---")
    
    # Cargar esquema definido en JSON (Contrato de Datos)
    with open(schema_path, "r") as file:
        schema_json = json.load(file)

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
        
    explicit_schema = StructType(fields)

    # 2. Leer Bronze usando Esquema Estricto y Modo Permisivo Controlado
    # Capturamos registros corruptos estructurales en la columna '_corrupt_record'
    df = spark.read \
        .option("pathGlobFilter", "*.csv") \
        .option("mode", "PERMISSIVE") \
        .option("columnNameOfCorruptRecord", "_corrupt_record") \
        .schema(explicit_schema) \
        .csv(bronze_path, header=True)
    
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

    # Limpieza de duplicados en datos sanos
    df_clean = df_valid.dropDuplicates()
    total_sin_duplicados = df_clean.count()
    duplicados_eliminados = total_leidos - total_sin_duplicados
        
    # --- LÓGICA DE NEGOCIO Y CUARENTENA FINANCIERA ---
    df_clean = df_clean.withColumn(
        "date_parsed",
        coalesce(
            to_date(col("date"), "yyyy-MM-dd"),
            to_date(col("date"), "yyyy/MM/dd"),
            to_date(col("date"), "dd/MM/yyyy"),
            to_date(col("date"), "MM/dd/yyyy")
        )
    ).withColumn("amount_parsed", col("amount").cast("double"))

    # Validación de reglas de dominio estrictas
    df_evaluated = df_clean.withColumn(
        "quarantine_reason",
        when(col("date_parsed").isNull(), lit("Fecha nula o formato no reconocido"))
        .when(col("amount_parsed").isNull(), lit("Monto nulo o no numérico"))
        .when(col("amount_parsed") < 0, lit("Monto negativo no permitido"))
        .otherwise(lit(None))
    ).withColumn("quarantined_at", current_timestamp())

    df_business_quarantine = df_evaluated.filter(col("quarantine_reason").isNotNull())
    df_sanitized = df_evaluated.filter(col("quarantine_reason").isNull())

    # Consolidar cuarentena 
    if df_structural_quarantine is not None and df_structural_quarantine.count() > 0:
        df_final_quarantine = df_structural_quarantine.unionByName(df_business_quarantine, allowMissingColumns=True)
    else:
        df_final_quarantine = df_business_quarantine

    total_quarantine = df_final_quarantine.count()
    
    # Escritura idempotente en cuarentena (overwrite sobre la partición del run_id actual)
    if total_quarantine > 0:
        print(f"Enviando {total_quarantine} registros a Cuarentena para el run_id: {run_id}")
        df_final_quarantine.write.mode("overwrite").parquet(quarantine_path)

    # Reestructurar DataFrame limpio final (sin rellenos arbitrarios de nulos)
    df_clean = df_sanitized.withColumn("date", col("date_parsed")) \
                           .withColumn("amount", col("amount_parsed")) \
                           .drop("date_parsed", "amount_parsed", "quarantine_reason", "quarantined_at")

    # Limpieza controlada de campos descriptivos (permitido según arquitectura)
    df_clean = df_clean.withColumn("transaction_description", trim(col("transaction_description").cast("string")))
    df_clean = df_clean.na.fill({"transaction_description": "Sin descripción"})
    
    # Normalización de categorías y tipos
    allowed_categories = [
        "Rent", "Travel", "Utilities", "Health & Fitness", "Shopping", 
        "Food & Drink", "Entertainment", "Salary", "Investment", "Other"
    ]
    df_clean = df_clean.withColumn("category", trim(initcap(col("category"))))
    df_clean = df_clean.withColumn(
        "category",
        when(col("category").isin(allowed_categories), col("category"))
        .otherwise("Other") 
    )
    
    allowed_types = ["Income", "Expense"]
    df_clean = df_clean.withColumn("type", trim(initcap(col("type"))))
    df_clean = df_clean.withColumn(
        "type",
        when(col("type").isin(allowed_types), col("type"))
        .otherwise("Expense") 
    )

    total_filas = df_clean.count()
    print(f"\n[MÉTRICAS] Total de registros listos para validar: {total_filas}")

    # Validación mediante Great Expectations
    print("Validando contra reglas de Great Expectations...")
    gx_df = SparkDFDataset(df_clean)

    for col_name, rules in schema_json.get("columns", {}).items():
        if rules.get("nullable") is False:
            gx_df.expect_column_values_to_not_be_null(col_name)
        
        if "allowed_values" in rules:
            gx_df.expect_column_values_to_be_in_set(col_name, rules["allowed_values"])

    validation_result = gx_df.validate()
    end_time = time.time()
    duracion = round(end_time - start_time, 2)

    reglas_evaluadas = validation_result["statistics"]["evaluated_expectations"]
    reglas_exitosas = validation_result["statistics"]["successful_expectations"]
    reglas_fallidas = validation_result["statistics"]["unsuccessful_expectations"]

    timestamp_actual = datetime.now().strftime('%Y-%m-%d %H:%M:%S')
    
    reporte_txt = f"""
    ==================================================
    REPORTE DE CALIDAD TRAS INGESTA (BRONZE -> SILVER)
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
    if reglas_fallidas > 0:
        reporte_txt += "\nDETALLE DE FALLOS EN SILVER:\n"
        for result in validation_result["results"]:
            if not result["success"]:
                exp_type = result['expectation_config']['expectation_type']
                column = result['expectation_config']['kwargs'].get('column', 'N/A')
                reporte_txt += f"  - Falló la regla '{exp_type}' en la columna '{column}'.\n"
                
    reporte_txt += "==================================================\n"
    print(reporte_txt)

    # 5. ESCRITURA EN SILVER
    if validation_result["success"]:
        print("ÉXITO: El dataset pasó todas las reglas de calidad estructurales.")
        df_clean.write.mode("overwrite").parquet(silver_path)
        print(f"Datos guardados exitosamente en: {silver_path}")
    else:
        print("ERROR: El dataset falló las validaciones de Great Expectations.")

    # Guardado de log físico local (respaldo)
    log_dir = os.path.join(BASE_DIR, "logs") 
    os.makedirs(log_dir, exist_ok=True) 
    nombre_archivo = f"reporte_dataset1_{datetime.now().strftime('%Y%m%d_%H%M%S')}.log"
    ruta_log = os.path.join(log_dir, nombre_archivo)
    
    with open(ruta_log, "w", encoding="utf-8") as archivo_log:
        archivo_log.write(reporte_txt)

    # Inserción automática en la base de datos PostgreSQL (tabla execution_logs)
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
        
        dataset_label = "Dataset 1" 
        
        pg_cursor.execute(insert_query, (
            dataset_label, run_id, duracion, total_leidos, 
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
    parser = argparse.ArgumentParser(description="Procesamiento Bronze a Silver con control de Esquema y Cuarentena")
    parser.add_argument("--run_id", required=True, help="Identificador único de ejecución (Run ID de Airflow)")
    args = parser.parse_args()
    
    process_transactions(args.run_id)