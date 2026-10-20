import json
import os
import re
import time
import argparse
import psycopg2
from datetime import datetime
from pyspark.sql import SparkSession
from pyspark.sql.types import StructType
from pyspark.sql.functions import col, when, sum as _sum, to_date, lit, current_timestamp
from great_expectations.dataset.sparkdf_dataset import SparkDFDataset

def to_snake_case(name):
    s1 = re.sub('(.)([A-Z][a-z]+)', r'\1_\2', name.replace(" ", "_"))
    s2 = re.sub('([a-z0-9])([A-Z])', r'\1_\2', s1).lower()
    return re.sub(r'_+', '_', s2)

def get_spark_session():
    spark = SparkSession.builder \
        .appName("BronzeToSilver_Dataset3_Con_MinIO") \
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

def process_financial_profile(run_id):
    start_time = time.time()
    spark = get_spark_session()
    
    # 1. Definir Rutas y Esquema Estricto
    BASE_DIR = os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
    schema_path = os.path.join(BASE_DIR, "schemas", "dataset3_schema.json")
    
    bronze_path = "s3a://bronze/Dataset3/*/*.csv"
    silver_path = "s3a://silver/financial_profile"
    quarantine_path = f"s3a://quarantine/Dataset3/run_id={run_id}"
    
    print(f"--- Iniciando transformación de financial_profile (Run ID: {run_id}) ---")
    
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

    # 3. Limpieza de nombres de columnas y duplicados en datos sanos
    for col_name in df_valid.columns:
        df_valid = df_valid.withColumnRenamed(col_name, to_snake_case(col_name))

    df_clean = df_valid.dropDuplicates()
    total_sin_duplicados = df_clean.count()
    duplicados_eliminados = total_leidos - total_sin_duplicados
        
    # --- PRE-PROCESAMIENTO PARA CUARENTENA DE NEGOCIO ---
    df_clean = df_clean.withColumn("record_date_parsed", to_date(col("record_date"), "yyyy-MM-dd"))
    df_clean = df_clean.withColumn(
        "has_loan_parsed", 
        when(col("has_loan") == "Yes", True)
        .when(col("has_loan") == "No", False)
        .otherwise(None).cast("boolean")
    )
    df_clean = df_clean.withColumn("loan_amount_usd_parsed", col("loan_amount_usd").cast("double"))

    # --- LÓGICA DE CUARENTENA DE NEGOCIO ---
    df_evaluated = df_clean.withColumn(
        "quarantine_reason",
        when(col("user_id").isNull() | (col("user_id") == ""), lit("ID de usuario nulo o vacío"))
        .when(col("record_date_parsed").isNull(), lit("Fecha nula o formato no reconocido"))
        .when((col("has_loan_parsed") == True) & col("loan_amount_usd_parsed").isNull(), lit("Préstamo activo pero monto nulo/inválido"))
        .when(col("age").isNull() | (col("age") < 18), lit("Edad nula o menor de edad"))
        .when(col("credit_score").isNull() | (col("credit_score") < 300) | (col("credit_score") > 850), lit("Credit score nulo o fuera de rango válido"))
        .otherwise(lit(None))
    ).withColumn("quarantined_at", current_timestamp())

    df_business_quarantine = df_evaluated.filter(col("quarantine_reason").isNotNull())
    df_sanitized = df_evaluated.filter(col("quarantine_reason").isNull())

    # Consolidar cuarentena total (Estructural + Negocio)
    if df_structural_quarantine is not None and df_structural_quarantine.count() > 0:
        df_final_quarantine = df_structural_quarantine.unionByName(df_business_quarantine, allowMissingColumns=True)
    else:
        df_final_quarantine = df_business_quarantine

    total_quarantine = df_final_quarantine.count()
    
    if total_quarantine > 0:
        print(f"⚠️ Enviando {total_quarantine} registros corruptos a Cuarentena (run_id: {run_id})...")
        df_final_quarantine.write.mode("overwrite").parquet(quarantine_path)

    # Continuar solo con registros sanitizados
    df_clean = df_sanitized.withColumn("record_date", col("record_date_parsed")) \
                           .withColumn("has_loan", col("has_loan_parsed")) \
                           .drop("record_date_parsed", "has_loan_parsed", "loan_amount_usd_parsed", "quarantine_reason", "quarantined_at")

    # Casteo seguro de tipos restantes
    columnas_double = [
        "monthly_income_usd", "monthly_expenses_usd", "savings_usd", 
        "loan_amount_usd", "monthly_emi_usd", "loan_interest_rate_pct", 
        "debt_to_income_ratio", "savings_to_income_ratio"
    ]
    for c in columnas_double:
        df_clean = df_clean.withColumn(c, col(c).cast("double"))

    columnas_int = ["age", "loan_term_months", "credit_score"]
    for c in columnas_int:
        df_clean = df_clean.withColumn(c, col(c).cast("integer"))

    # Manejo de nulos controlado únicamente para variables opcionales
    df_clean = df_clean.na.fill({
        "monthly_income_usd": 0.0,
        "monthly_expenses_usd": 0.0,
        "savings_usd": 0.0
    })

    # Clamping proactivo para asegurar rangos lógicos monetarios
    monetarios_no_negativos = ["monthly_income_usd", "monthly_expenses_usd", "debt_to_income_ratio", "savings_usd"]
    for c in monetarios_no_negativos:
        df_clean = df_clean.withColumn(c, when(col(c) < 0.0, 0.0).otherwise(col(c)))

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

    gx_df.expect_column_values_to_be_between("age", min_value=18)
    gx_df.expect_column_values_to_be_between("monthly_income_usd", min_value=0.0)
    gx_df.expect_column_values_to_be_between("monthly_expenses_usd", min_value=0.0)
    gx_df.expect_column_values_to_be_between("credit_score", min_value=300, max_value=850)
    gx_df.expect_column_values_to_be_between("debt_to_income_ratio", min_value=0.0)
    gx_df.expect_column_values_to_not_be_null("record_date")

    validation_result = gx_df.validate()
    end_time = time.time()
    duracion = round(end_time - start_time, 2)

    reglas_evaluadas = validation_result["statistics"]["evaluated_expectations"]
    reglas_exitosas = validation_result["statistics"]["successful_expectations"]
    reglas_fallidas = validation_result["statistics"]["unsuccessful_expectations"]

    timestamp_actual = datetime.now().strftime('%Y-%m-%d %H:%M:%S')
    
    reporte_txt = f"""
    ==================================================
    REPORTE DE CALIDAD TRAS INGESTA (BRONZE -> SILVER - DATASET 3)
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

    # 5. ESCRITURA EN SILVER Y REGISTRO EN POSTGRESQL
    if validation_result["success"]:
        print("ÉXITO: El dataset 3 pasó todas las reglas de calidad.")
        df_clean.write.mode("overwrite").parquet(silver_path)
        print(f"Datos guardados exitosamente en: {silver_path}")
    else:
        print("ERROR: El dataset 3 no pasó las validaciones de calidad. No se escribió en Silver.")

    # Log físico local (respaldo)
    log_dir = os.path.join(BASE_DIR, "logs")
    os.makedirs(log_dir, exist_ok=True) 
    nombre_archivo = f"reporte_dataset3_{datetime.now().strftime('%Y%m%d_%H%M%S')}.log"
    ruta_log = os.path.join(log_dir, nombre_archivo)
    
    with open(ruta_log, "w", encoding="utf-8") as archivo_log:
        archivo_log.write(reporte_txt)
        
    print(f"Reporte de calidad guardado físicamente en: {ruta_log}")

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
        
        dataset_label = "Dataset 3" 
        
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
    parser = argparse.ArgumentParser(description="Procesamiento Bronze a Silver Dataset 3 con control de Esquema y Cuarentena")
    parser.add_argument("--run_id", required=True, help="Identificador único de ejecución (Run ID de Airflow)")
    args = parser.parse_args()
    
    process_financial_profile(args.run_id)