import json
import os
import time
import re
from datetime import datetime
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, when, to_date, trim, initcap, coalesce, lit, current_timestamp
from pyspark.sql.functions import sum as _sum
from great_expectations.dataset.sparkdf_dataset import SparkDFDataset

def get_spark_session():
    return SparkSession.builder \
        .appName("BronzeToSilver_PersonalTransactions") \
        .config("spark.sql.parquet.writeLegacyFormat", "true") \
        .config("spark.sql.ansi.enabled", "false") \
        .getOrCreate()

def process_transactions():
    start_time = time.time()
    spark = get_spark_session()
    
    # 1. Definir Rutas
    BASE_DIR = os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
    
    bronze_path = os.path.join(BASE_DIR, "data", "bronze", "Dataset1") + "/"
    silver_path = os.path.join(BASE_DIR, "data", "silver", "personal_transactions")
    quarantine_path = os.path.join(BASE_DIR, "data", "quarantine", "Dataset1") # Ruta de aislamiento
    schema_path = os.path.join(BASE_DIR, "schemas", "dataset1_schema.json")
    
    print("--- Iniciando transformación de personal_transactions ---")
    
    # 2. Leer Bronze 
    df = spark.read \
        .option("pathGlobFilter", "*.csv") \
        .csv(bronze_path, header=True, inferSchema=True)
    
    # 3. LIMPIEZA Y TRANSFORMACIÓN (PySpark)
    def to_snake_case(name):
        s1 = re.sub('(.)([A-Z][a-z]+)', r'\1_\2', name.replace(" ", "_"))
        s2 = re.sub('([a-z0-9])([A-Z])', r'\1_\2', s1).lower()
        return re.sub(r'_+', '_', s2)

    for col_name in df.columns:
        df = df.withColumnRenamed(col_name, to_snake_case(col_name))

    total_leidos = df.count()
    df_clean = df.dropDuplicates()
    total_sin_duplicados = df_clean.count()
    duplicados_eliminados = total_leidos - total_sin_duplicados
        
    # --- PRE-PROCESAMIENTO PARA CUARENTENA ---
    df_clean = df_clean.withColumn(
        "date_parsed",
        coalesce(
            to_date(col("date"), "yyyy-MM-dd"),
            to_date(col("date"), "yyyy/MM/dd"),
            to_date(col("date"), "dd/MM/yyyy"),
            to_date(col("date"), "MM/dd/yyyy")
        )
    ).withColumn("amount_parsed", col("amount").cast("double"))

    # --- LÓGICA DE CUARENTENA (Aislamiento fila por fila) ---
    df_evaluated = df_clean.withColumn(
        "quarantine_reason",
        when(col("date_parsed").isNull(), lit("Fecha nula o formato no reconocido"))
        .when(col("amount_parsed").isNull(), lit("Monto nulo o no numérico"))
        .otherwise(lit(None))
    ).withColumn("quarantined_at", current_timestamp())

    # Bifurcación
    df_quarantine = df_evaluated.filter(col("quarantine_reason").isNotNull())
    df_valid = df_evaluated.filter(col("quarantine_reason").isNull())

    # Escribir registros corruptos a cuarentena (si existen)
    total_quarantine = df_quarantine.count()
    if total_quarantine > 0:
        print(f"⚠️ Enviando {total_quarantine} registros corruptos a Cuarentena...")
        df_quarantine.write.mode("append").parquet(quarantine_path)

    # Restaurar estructura original solo con datos sanos para continuar el flujo
    df_clean = df_valid.withColumn("date", col("date_parsed")) \
                       .withColumn("amount", col("amount_parsed")) \
                       .drop("date_parsed", "amount_parsed", "quarantine_reason", "quarantined_at")
    # ---------------------------------------------------------

    # c. Limpieza: transaction_description 
    df_clean = df_clean.withColumn("transaction_description", trim(col("transaction_description").cast("string")))
    df_clean = df_clean.na.fill({"transaction_description": "Sin descripción"})
    
    # d. Limpieza: amount (Respaldo por si quedan nulos lógicos)
    df_clean = df_clean.na.fill({"amount": 0.0})
    
    # e. Limpieza: category
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
    
    # f. Limpieza: type
    allowed_types = ["Income", "Expense"]
    df_clean = df_clean.withColumn("type", trim(initcap(col("type"))))
    df_clean = df_clean.withColumn(
        "type",
        when(col("type").isin(allowed_types), col("type"))
        .otherwise("Expense") 
    )

    total_filas = df_clean.count()
    print(f"\n[MÉTRICAS] Total de registros listos para validar: {total_filas}")

    print("[MÉTRICAS] Conteo de valores nulos por columna:")
    df_clean.select([_sum(col(c).isNull().cast("int")).alias(c) for c in df_clean.columns]).show()

    print("Validando contra reglas de Great Expectations...")
    gx_df = SparkDFDataset(df_clean)

    with open(schema_path, "r") as file:
        custom_schema = json.load(file)

    for col_name, rules in custom_schema.get("columns", {}).items():
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

    # 1. Empaquetamos todo el reporte actualizando las métricas de cuarentena
    timestamp_actual = datetime.now().strftime('%Y-%m-%d %H:%M:%S')
    
    reporte_txt = f"""
    ==================================================
    REPORTE DE CALIDAD TRAS INGESTA (BRONZE -> SILVER)
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
        reporte_txt += "\nDETALLE DE FALLOS EN SILVER:\n"
        for result in validation_result["results"]:
            if not result["success"]:
                exp_type = result['expectation_config']['expectation_type']
                column = result['expectation_config']['kwargs'].get('column', 'N/A')
                reporte_txt += f"  - Falló la regla '{exp_type}' en la columna '{column}'.\n"
                
    reporte_txt += "==================================================\n"

    # 2. Lo imprimimos en la consola 
    print(reporte_txt)

    # 3. Lo guardamos físicamente
    log_dir = os.path.join(BASE_DIR, "logs") 
    os.makedirs(log_dir, exist_ok=True) 
    
    nombre_archivo = f"reporte_dataset1_{datetime.now().strftime('%Y%m%d_%H%M%S')}.log"
    ruta_log = os.path.join(log_dir, nombre_archivo)
    
    with open(ruta_log, "w", encoding="utf-8") as archivo_log:
        archivo_log.write(reporte_txt)
        
    print(f"Reporte de calidad guardado físicamente en: {ruta_log}")

    # 5. ESCRITURA EN SILVER
    if validation_result["success"]:
        print("ÉXITO: El dataset pasó todas las reglas de calidad estructurales.")
        df_clean.write.mode("overwrite").parquet(silver_path)
        print(f"Datos guardados exitosamente en: {silver_path}")
    else:
        print("ERROR: El dataset falló las validaciones de Great Expectations.")
                
    spark.stop()

if __name__ == "__main__":
    process_transactions()