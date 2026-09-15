import json
import os
import re
import time
from datetime import datetime
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, when, sum as _sum, to_date, lit, current_timestamp
from great_expectations.dataset.sparkdf_dataset import SparkDFDataset

def to_snake_case(name):
    s1 = re.sub('(.)([A-Z][a-z]+)', r'\1_\2', name.replace(" ", "_"))
    s2 = re.sub('([a-z0-9])([A-Z])', r'\1_\2', s1).lower()
    return re.sub(r'_+', '_', s2)

def get_spark_session():
    return SparkSession.builder \
        .appName("BronzeToSilver_FinancialProfile") \
        .config("spark.sql.parquet.writeLegacyFormat", "true") \
        .config("spark.sql.ansi.enabled", "false") \
        .getOrCreate()

def process_financial_profile():
    start_time = time.time()
    spark = get_spark_session()
    
    # 1. Definir Rutas (Absolutas dinámicas)
    BASE_DIR = os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
    
    bronze_path = os.path.join(BASE_DIR, "data", "bronze", "Dataset3") + "/"
    silver_path = os.path.join(BASE_DIR, "data", "silver", "financial_profile")
    quarantine_path = os.path.join(BASE_DIR, "data", "quarantine", "Dataset3")
    schema_path = os.path.join(BASE_DIR, "schemas", "dataset3_schema.json")
    
    print("--- Iniciando transformación de financial_profile ---")
    
    # 2. Leer Bronze 
    df = spark.read \
        .option("pathGlobFilter", "*.csv") \
        .csv(bronze_path, header=True, inferSchema=True)
    
    # 3. LIMPIEZA Y TRANSFORMACIÓN
    for col_name in df.columns:
        df = df.withColumnRenamed(col_name, to_snake_case(col_name))

    total_leidos = df.count()
    df_clean = df.dropDuplicates()
    total_sin_duplicados = df_clean.count()
    duplicados_eliminados = total_leidos - total_sin_duplicados
        
    # --- PRE-PROCESAMIENTO PARA CUARENTENA ---
    df_clean = df_clean.withColumn("record_date_parsed", to_date(col("record_date"), "yyyy-MM-dd"))
    df_clean = df_clean.withColumn(
        "has_loan_parsed", 
        when(col("has_loan") == "Yes", True)
        .when(col("has_loan") == "No", False)
        .otherwise(None).cast("boolean")
    )
    df_clean = df_clean.withColumn("loan_amount_usd_parsed", col("loan_amount_usd").cast("double"))

    # --- LÓGICA DE CUARENTENA ---
    df_evaluated = df_clean.withColumn(
        "quarantine_reason",
        when(col("user_id").isNull() | (col("user_id") == ""), lit("ID de usuario nulo o vacío"))
        .when(col("record_date_parsed").isNull(), lit("Fecha nula o formato no reconocido"))
        .when((col("has_loan_parsed") == True) & col("loan_amount_usd_parsed").isNull(), lit("Préstamo activo pero monto nulo/inválido"))
        .otherwise(lit(None))
    ).withColumn("quarantined_at", current_timestamp())

    df_quarantine = df_evaluated.filter(col("quarantine_reason").isNotNull())
    df_valid = df_evaluated.filter(col("quarantine_reason").isNull())

    total_quarantine = df_quarantine.count()
    if total_quarantine > 0:
        print(f"⚠️ Enviando {total_quarantine} registros corruptos a Cuarentena...")
        df_quarantine.write.mode("append").parquet(quarantine_path)

    df_clean = df_valid.withColumn("record_date", col("record_date_parsed")) \
                       .withColumn("has_loan", col("has_loan_parsed")) \
                       .drop("record_date_parsed", "has_loan_parsed", "loan_amount_usd_parsed", "quarantine_reason", "quarantined_at")

    # c. Limpieza: Casteo de tipos restantes
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

    # f. Manejo de Nulos 
    df_clean = df_clean.na.fill({
        "monthly_income_usd": 0.0,
        "monthly_expenses_usd": 0.0,
        "savings_usd": 0.0,
        "credit_score": 300,
        "age": 18
    })

    # g. Clamping Proactivo
    monetarios_no_negativos = ["monthly_income_usd", "monthly_expenses_usd", "debt_to_income_ratio", "savings_usd"]
    for c in monetarios_no_negativos:
        df_clean = df_clean.withColumn(c, when(col(c) < 0.0, 0.0).otherwise(col(c)))

    df_clean = df_clean.withColumn("age", when(col("age") < 18, 18).otherwise(col("age")))
    
    df_clean = df_clean.withColumn(
        "credit_score", 
        when(col("credit_score") < 300, 300)
        .when(col("credit_score") > 850, 850)
        .otherwise(col("credit_score"))
    )

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

    #Log
    log_dir = os.path.join(BASE_DIR, "logs")
    os.makedirs(log_dir, exist_ok=True) 
    nombre_archivo = f"reporte_dataset3_{datetime.now().strftime('%Y%m%d_%H%M%S')}.log"
    ruta_log = os.path.join(log_dir, nombre_archivo)
    
    with open(ruta_log, "w", encoding="utf-8") as archivo_log:
        archivo_log.write(reporte_txt)
        
    print(f"Reporte de calidad guardado físicamente en: {ruta_log}")

    if validation_result["success"]:
        print("ÉXITO: El dataset 3 pasó todas las reglas de calidad.")
        df_clean.write.mode("overwrite").parquet(silver_path)
        print(f"Datos guardados exitosamente en: {silver_path}")
    else:
        print("ERROR: El dataset 3 no pasó las validaciones de calidad. No se escribió en Silver.")
                
    spark.stop()

if __name__ == "__main__":
    process_financial_profile()