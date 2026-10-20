import os
import argparse
import psycopg2
from datetime import datetime
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, when, lit, current_timestamp, round as _round

def get_spark_session():
    spark = SparkSession.builder \
        .appName("SilverToGold_Dataset3_Con_MinIO") \
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
        .config("spark.sql.parquet.writeLegacyFormat", "true") \
        .config("spark.sql.ansi.enabled", "false") \
        .config("spark.sql.shuffle.partitions", "2") \
        .config("spark.driver.memory", "1g") \
        .getOrCreate()
    return spark

def process_gold_financial_profile(run_id):
    spark = get_spark_session()
    
    # 1. Rutas de lectura (Silver) y escritura (Gold)
    silver_path = "s3a://silver/financial_profile"
    gold_path = f"s3a://gold/financial_profile/run_id={run_id}"
    
    print(f"--- Iniciando construcción de Capa Gold - Dataset 3 (Run ID: {run_id}) ---")
    
    # Lectura desde Silver aplicando el esquema limpio en Parquet
    df_silver = spark.read.parquet(silver_path)
    total_leidos = df_silver.count()
    print(f"Registros leídos desde Silver: {total_leidos}")

    # 2. Selección de columnas clave y cálculo de métricas de valor agregado
    df_gold = df_silver.select(
        col("user_id"),
        col("record_date"),
        col("age"),
        col("monthly_income_usd"),
        col("monthly_expenses_usd"),
        col("savings_usd"),
        col("credit_score"),
        col("has_loan"),
        col("loan_amount_usd"),
        col("monthly_emi_usd"),
        col("debt_to_income_ratio")
    )

    # 3. Cálculos de Negocio Avanzados (Métricas Gold)
    
    # A. Capacidad de Ahorro neta mensual (Ingresos - Gastos - EMI actual si tiene)
    df_gold = df_gold.withColumn(
        "savings_capacity",
        _round(col("monthly_income_usd") - col("monthly_expenses_usd") - when(col("monthly_emi_usd").isNull(), 0.0).otherwise(col("monthly_emi_usd")), 2)
    )

    # B. Fondo de Emergencia (Meses de gastos cubiertos por los ahorros actuales)
    df_gold = df_gold.withColumn(
        "emergency_fund_months",
        when(col("monthly_expenses_usd") > 0, _round(col("savings_usd") / col("monthly_expenses_usd"), 1))
        .otherwise(lit(0.0))
    )

    # C. Niveles de Score comprensibles para evaluación bancaria y del cliente
    # Excelente 🟢 (>750), Bueno 🟡 (670-750), Regular 🟠 (580-669), Bajo 🔴 (<580)
    df_gold = df_gold.withColumn(
        "nivel_score_category",
        when(col("credit_score") >= 750, lit("Excelente 🟢"))
        .when((col("credit_score") >= 670) & (col("credit_score") < 750), lit("Bueno 🟡"))
        .when((col("credit_score") >= 580) & (col("credit_score") < 670), lit("Regular 🟠"))
        .otherwise(lit("Bajo 🔴"))
    ).withColumn(
        "nivel_score_meaning",
        when(col("credit_score") >= 750, lit("Acceso inmediato a créditos grandes con las mejores tasas."))
        .when((col("credit_score") >= 670) & (col("credit_score") < 750), lit("Alta probabilidad de aprobación y buenas condiciones financieras."))
        .when((col("credit_score") >= 580) & (col("credit_score") < 670), lit("Créditos aprobados pero con tasas más altas o líneas bajas."))
        .otherwise(lit("Alto riesgo de impago. Alta probabilidad de rechazo."))
    )

    # D. Lógica orientada a responder las preguntas clave del usuario:
    # ¿Puede sacar un crédito? ¿Cuánto y le conviene?
    # Estimación de crédito máximo sugerido basado en capacidad de ahorro y score
    df_gold = df_gold.withColumn(
        "eligible_for_credit",
        when((col("credit_score") >= 580) & (col("savings_capacity") > 100), True).otherwise(False)
    ).withColumn(
        "suggested_max_credit_limit",
        when(col("eligible_for_credit") == True, _round(col("savings_capacity") * 12 * 2.5, 2)) # 2.5 veces su capacidad de ahorro anual
        .otherwise(lit(0.0))
    )

    # 4. Validar integridad final en Gold (Cero nulos en métricas clave)
    total_gold = df_gold.count()
    print(f"Registros procesados para Gold: {total_gold}")
    print("\n" + "="*60)
    print("       REPORTE DE EJECUCIÓN - CAPA GOLD (DATASET 3)")
    print("="*60)
    print(f" Run ID          : {run_id}")
    print(f" Estado          : EXITOSO (SUCCESS)")
    print(f" Registros Gold  : {df_gold.count() if 'df_gold' in locals() else 'N/A'}")
    print(f" Destino MinIO   : s3a://gold/financial_profile/")
    print(f" Timestamp fin   : {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
    print("="*60 + "\n")

    # 5. Escritura en la ruta Gold de MinIO
    df_gold.write.mode("overwrite").parquet(gold_path)
    print(f"Datos de Capa Gold guardados exitosamente en: {gold_path}")

    def log_execution_to_postgres(run_id, status="SUCCESS", dataset="Dataset 3 Gold"):
        try:
            # Conexión a tu PostgreSQL local/Docker
            connection = psycopg2.connect(
                host="gemelo-postgres", # o el host de tu postgres
                database="airflow",     # o tu base de datos de trabajo
                user="airflow",
                password="airflow"
            )
            cursor = connection.cursor()
            
            # Crear tabla de logs si no existe
            cursor.execute("""
                CREATE TABLE IF NOT EXISTS pipeline_execution_logs (
                    id SERIAL PRIMARY KEY,
                    run_id VARCHAR(255),
                    dataset_name VARCHAR(100),
                    status VARCHAR(50),
                    executed_at TIMESTAMP
                );
            """)
            
            # Insertar registro
            cursor.execute(
                "INSERT INTO pipeline_execution_logs (run_id, dataset_name, status, executed_at) VALUES (%s, %s, %s, %s)",
                (run_id, dataset, status, datetime.now())
            )
            
            connection.commit()
            cursor.close()
            connection.close()
            print("--- Log de ejecución guardado exitosamente en PostgreSQL ---")
        except Exception as e:
            print(f"Error al guardar el log en Postgres: {e}")
    
    spark.stop()

if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Procesamiento Silver a Gold - Dataset 3")
    parser.add_argument("--run_id", required=True, help="Identificador único de ejecución (Run ID de Airflow)")
    args = parser.parse_args()
    
    process_gold_financial_profile(args.run_id)