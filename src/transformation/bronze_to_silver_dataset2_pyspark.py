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
from great_expectations.dataset.sparkdf_dataset import SparkDFDataset

def to_snake_case(name):
    s1 = re.sub('(.)([A-Z][a-z]+)', r'\1_\2', name.replace(" ", "_"))
    s2 = re.sub('([a-z0-9])([A-Z])', r'\1_\2', s1).lower()
    return re.sub(r'_+', '_', s2)

def get_spark_session():
    spark = SparkSession.builder \
        .appName("BronzeToSilver_Dataset2_Con_MinIO") \
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

def process_credit_cards(run_id):
    start_time = time.time()
    spark = get_spark_session()
    
    # 1. Definir Rutas y Esquema Estricto
    BASE_DIR = os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
    schema_path = os.path.join(BASE_DIR, "schemas", "dataset2_schema.json")
    
    bronze_path = "s3a://bronze/Dataset2/*/*.csv"
    silver_path = "s3a://silver/credit_card_balances"
    quarantine_path = f"s3a://quarantine/Dataset2/run_id={run_id}"
    
    print(f"--- Iniciando transformación de credit_card_balances (Run ID: {run_id}) ---")
    
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

    # 2. Leer Bronze con Esquema Estricto y Modo Permisivo Controlado
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

    # 3. Limpieza de nombres y duplicados
    for col_name in df_valid.columns:
        df_valid = df_valid.withColumnRenamed(col_name, to_snake_case(col_name))

    df_clean = df_valid.dropDuplicates()
    total_sin_duplicados = df_clean.count()
    duplicados_eliminados = total_leidos - total_sin_duplicados

    # --- LÓGICA DE CUARENTENA ---
    df_evaluated = df_clean.withColumn(
        "quarantine_reason",
        when(col("cust_id").isNull() | (col("cust_id") == ""), lit("ID de cliente nulo o vacío"))
        .when(col("credit_limit").isNull() | (col("credit_limit") <= 0), lit("Límite de crédito inválido o <= 0"))
        .otherwise(lit(None))
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
    # -----------------------------
        
    # Limpieza secundaria (Imputaciones lógicas y Clamping)
    columnas_financieras = [
        "balance", "purchases", "oneoff_purchases", "installments_purchases", 
        "cash_advance", "payments", "minimum_payments"
    ]
    
    for c in columnas_financieras:
        df_clean = df_clean.withColumn(c, col(c).cast("double"))
        df_clean = df_clean.na.fill({c: 0.0})
        df_clean = df_clean.withColumn(c, when(col(c) < 0.0, 0.0).otherwise(col(c)))

    frecuencias_a_topar = [
        "balance_frequency", "purchases_frequency", "oneoff_purchases_frequency", 
        "purchases_installments_frequency", "cash_advance_frequency", "prc_full_payment"
    ]
    for c in frecuencias_a_topar:
        df_clean = df_clean.withColumn(c, when(col(c) > 1.0, 1.0).when(col(c) < 0.0, 0.0).otherwise(col(c)))

    df_clean = df_clean.withColumn("tenure", when(col("tenure") < 1, 1).otherwise(col("tenure")))

    # MÉTRICAS Y GREAT EXPECTATIONS
    total_filas = df_clean.count()
    print(f"\n[MÉTRICAS] Total de registros listos para validar: {total_filas}")

    print("Validando contra reglas de Great Expectations...")
    gx_df = SparkDFDataset(df_clean)

    for col_name, rules in schema_json.get("columns", {}).items():
        if rules.get("nullable") is False:
            gx_df.expect_column_values_to_not_be_null(col_name)
        if rules.get("unique") is True:
            gx_df.expect_column_values_to_be_unique(col_name)
    
    frecuencias = [c for c in schema_json["columns"].keys() if "frequency" in c or c == "prc_full_payment"]
    for col_freq in frecuencias:
        gx_df.expect_column_values_to_be_between(col_freq, min_value=0.0, max_value=1.0)

    monetarios = ["balance", "purchases", "oneoff_purchases", "installments_purchases", "cash_advance", "payments", "minimum_payments"]
    for col_mon in monetarios:
        gx_df.expect_column_values_to_be_between(col_mon, min_value=0.0)

    gx_df.expect_column_values_to_be_between("credit_limit", min_value=0.0)
    gx_df.expect_column_values_to_be_between("tenure", min_value=1)

    validation_result = gx_df.validate()
    end_time = time.time()
    duracion = round(end_time - start_time, 2)

    reglas_evaluadas = validation_result["statistics"]["evaluated_expectations"]
    reglas_exitosas = validation_result["statistics"]["successful_expectations"]
    reglas_fallidas = validation_result["statistics"]["unsuccessful_expectations"]

    timestamp_actual = datetime.now().strftime('%Y-%m-%d %H:%M:%S')
    
    reporte_txt = f"""
    ==================================================
    REPORTE DE CALIDAD TRAS INGESTA (BRONZE -> SILVER - DATASET 2)
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
    """

    if reglas_fallidas > 0:
        reporte_txt += "\nDETALLE DE FALLOS:\n"
        for result in validation_result["results"]:
            if not result["success"]:
                exp_type = result['expectation_config']['expectation_type']
                column = result['expectation_config']['kwargs'].get('column', 'N/A')
                reporte_txt += f"  - Falló la regla '{exp_type}' en la columna '{column}'.\n"
                
    reporte_txt += "==================================================\n"

    print(reporte_txt)

    # Escritura de log físico local
    log_dir = os.path.join(BASE_DIR, "logs")
    os.makedirs(log_dir, exist_ok=True) 
    nombre_archivo = f"reporte_dataset2_{datetime.now().strftime('%Y%m%d_%H%M%S')}.log"
    ruta_log = os.path.join(log_dir, nombre_archivo)
    
    with open(ruta_log, "w", encoding="utf-8") as archivo_log:
        archivo_log.write(reporte_txt)
        
    print(f"Reporte de calidad guardado físicamente en: {ruta_log}")

    # 5. ESCRITURA EN SILVER Y REGISTRO AUTOMÁTICO EN POSTGRESQL
    if validation_result["success"]:
        print("ÉXITO: El dataset 2 pasó todas las reglas de calidad.")
        df_clean.write.mode("overwrite").parquet(silver_path)
        print(f"Datos guardados exitosamente en: {silver_path}")
    else:
        print("ERROR: El dataset 2 no pasó las validaciones de calidad.")

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
            "Dataset 2", run_id, duracion, total_leidos, 
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
    parser = argparse.ArgumentParser(description="Procesamiento Bronze a Silver Dataset 2 con control de Esquema y Cuarentena")
    parser.add_argument("--run_id", required=True, help="Identificador único de ejecución (Run ID de Airflow)")
    args = parser.parse_args()
    
    process_credit_cards(args.run_id)