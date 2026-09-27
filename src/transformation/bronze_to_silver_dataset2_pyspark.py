import json
import os
import re
import time
from datetime import datetime
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, when, sum as _sum, lit, current_timestamp
from great_expectations.dataset.sparkdf_dataset import SparkDFDataset

def to_snake_case(name):
    s1 = re.sub('(.)([A-Z][a-z]+)', r'\1_\2', name.replace(" ", "_"))
    s2 = re.sub('([a-z0-9])([A-Z])', r'\1_\2', s1).lower()
    return re.sub(r'_+', '_', s2)

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

def process_credit_cards():
    start_time = time.time()
    spark = get_spark_session()
    
    # 1. Definir Rutas (Absolutas dinámicas)
    BASE_DIR = os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
    schema_path = os.path.join(BASE_DIR, "schemas", "dataset2_schema.json")
    
    bronze_path = "s3a://bronze/Dataset2/*/*.csv"
    silver_path = "s3a://silver/credit_card_balances"
    quarantine_path = "s3a://quarantine/Dataset2"
    
    print("--- Iniciando transformación de credit_card_balances ---")
    
    # 2. Leer Bronze 
    df = spark.read \
        .option("pathGlobFilter", "*.csv") \
        .csv(bronze_path, header=True, inferSchema=True)
    
    # 3. LIMPIEZA BÁSICA
    for col_name in df.columns:
        df = df.withColumnRenamed(col_name, to_snake_case(col_name))

    total_leidos = df.count()
    df_clean = df.dropDuplicates()
    total_sin_duplicados = df_clean.count()
    duplicados_eliminados = total_leidos - total_sin_duplicados

    df_clean = df_clean.withColumn("cust_id", col("cust_id").cast("string"))
    df_clean = df_clean.withColumn("credit_limit", col("credit_limit").cast("double"))

    # --- LÓGICA DE CUARENTENA ---
    # Identificamos errores críticos según arquitectura (ID nulo o Límite <= 0)
    df_evaluated = df_clean.withColumn(
        "quarantine_reason",
        when(col("cust_id").isNull() | (col("cust_id") == ""), lit("ID de cliente nulo o vacío"))
        .when(col("credit_limit").isNull() | (col("credit_limit") <= 0), lit("Límite de crédito inválido o <= 0"))
        .otherwise(lit(None))
    ).withColumn("quarantined_at", current_timestamp())

    # Bifurcación
    df_quarantine = df_evaluated.filter(col("quarantine_reason").isNotNull())
    df_valid = df_evaluated.filter(col("quarantine_reason").isNull())

    total_quarantine = df_quarantine.count()
    if total_quarantine > 0:
        print(f"⚠️ Enviando {total_quarantine} registros corruptos a Cuarentena...")
        df_quarantine.write.mode("append").parquet(quarantine_path)

    # Continuamos solo con los datos sanos
    df_clean = df_valid.drop("quarantine_reason", "quarantined_at")
    # -----------------------------
        
    # Limpieza secundaria (Imputaciones y Clamping para los datos sanos)
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

    with open(schema_path, "r") as file:
        custom_schema = json.load(file)

    for col_name, rules in custom_schema.get("columns", {}).items():
        if rules.get("nullable") is False:
            gx_df.expect_column_values_to_not_be_null(col_name)
        if rules.get("unique") is True:
            gx_df.expect_column_values_to_be_unique(col_name)
    
    frecuencias = [c for c in custom_schema["columns"].keys() if "frequency" in c or c == "prc_full_payment"]
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

    # Escritura de log 
    log_dir = os.path.join(BASE_DIR, "logs")
    os.makedirs(log_dir, exist_ok=True) 
    nombre_archivo = f"reporte_dataset2_{datetime.now().strftime('%Y%m%d_%H%M%S')}.log"
    ruta_log = os.path.join(log_dir, nombre_archivo)
    
    with open(ruta_log, "w", encoding="utf-8") as archivo_log:
        archivo_log.write(reporte_txt)
        
    print(f"Reporte de calidad guardado físicamente en: {ruta_log}")

    if validation_result["success"]:
        print("ÉXITO: El dataset 2 pasó todas las reglas de calidad.")
        df_clean.write.mode("overwrite").parquet(silver_path)
        print(f"Datos guardados exitosamente en: {silver_path}")
    else:
        print("ERROR: El dataset 2 no pasó las validaciones de calidad.")
                
    spark.stop()

if __name__ == "__main__":
    process_credit_cards()