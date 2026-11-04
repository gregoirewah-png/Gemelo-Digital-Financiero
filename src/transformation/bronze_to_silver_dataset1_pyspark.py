import json
import os
import re
import time
import argparse
from datetime import datetime
import psycopg2
from pyspark.storagelevel import StorageLevel
from pyspark.sql import SparkSession
from pyspark.sql.types import (StringType,
    DoubleType,
    IntegerType,
    DateType,
    StructField,
    StructType)

from pyspark.sql.functions import (
    col, when, lit, current_timestamp,
    trim, upper, initcap, concat_ws, length,
    sum as _sum
)

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
    
    # Sanear el run_id para evitar errores de URI en Hadoop S3A
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

        # No duplicar la columna reservada de Spark
        if col_name.lower() == "_corrupt_record":
            continue

        data_type_str = rules.get("type", "string").lower()
        spark_type = type_mapping.get(data_type_str, StringType())
        is_nullable = rules.get("nullable", True)

        fields.append(
            StructField(col_name, spark_type, is_nullable)
        )

    # Agregarla exactamente una vez
    fields.append(
        StructField("_corrupt_record", StringType(), True)
    )

    explicit_schema = StructType(fields)

    # 2. Leer Bronze con Esquema Estricto y Modo Permisivo Controlado
    reader = spark.read \
        .option("pathGlobFilter", "*.csv") \
        .option("mode", "PERMISSIVE") \
        .option("columnNameOfCorruptRecord", "_corrupt_record")
        
    if explicit_schema:
        df = reader.schema(explicit_schema).csv(
            bronze_path,
            header=True
        )
    else:
        df = reader.csv(
            bronze_path,
            header=True
        )

    # Materializar los datos antes de consultar _corrupt_record
    df = df.persist(StorageLevel.MEMORY_AND_DISK)

    # Esta acción carga el DataFrame en caché
    total_leidos = df.count()

    print(f"Registros leídos desde Bronze: {total_leidos}")
    print(f"Columnas detectadas: {df.columns}")
    
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

    # 3. Estandarización de nombres
    for col_name in df_valid.columns:
        if col_name != "_corrupt_record":
            df_valid = df_valid.withColumnRenamed(
                col_name,
                to_snake_case(col_name)
            )

    # Conservar el total de registros procesables antes de deduplicar
    registros_antes_dedup = df_valid.count()

    # Normalización de valores
    df_clean = (
        df_valid
        .withColumn(
            "transaction_description",
            trim(col("transaction_description"))
        )
        .withColumn(
            "category",
            initcap(trim(col("category")))
        )
        .withColumn(
            "type",
            when(
                upper(trim(col("type"))) == "INCOME",
                lit("Income")
            )
            .when(
                upper(trim(col("type"))) == "EXPENSE",
                lit("Expense")
            )
            .otherwise(trim(col("type")))
        )
    )

    # Si la normalización hace que dos registros sean idénticos,
    # también deben considerarse duplicados.
    df_clean = df_clean.dropDuplicates()

    total_sin_duplicados = df_clean.count()

    duplicados_eliminados = (
        registros_antes_dedup - total_sin_duplicados
    )

    # --- LÓGICA DE CUARENTENA ---
    # 1. Definir reglas de calidad
    reglas = {
        "fecha_invalida": (
            col("date").isNull()
        ),

        "descripcion_vacia": (
            col("transaction_description").isNull() |
            (trim(col("transaction_description")) == "")
        ),

        "categoria_no_permitida": (
            col("category").isNull() |
            (~col("category").isin(
                "Rent",
                "Travel",
                "Utilities",
                "Health & Fitness",
                "Shopping",
                "Food & Drink",
                "Entertainment",
                "Salary",
                "Investment",
                "Other"
            ))
        ),

        "importe_invalido": (
            col("amount").isNull()
        ),

        "tipo_no_permitido": (
            col("type").isNull() |
            (~col("type").isin("Income", "Expense"))
        )
    }

    # 2. Crear una descripción de los errores encontrados
    razones = [
        when(condicion, lit(nombre))
        for nombre, condicion in reglas.items()
    ]

    df_evaluated = (
        df_clean
        .withColumn(
            "_reason_parts",
            concat_ws(" | ", *razones)
        )
        .withColumn(
            "quarantine_reason",
            when(
                length(col("_reason_parts")) > 0,
                col("_reason_parts")
            ).otherwise(lit(None).cast("string"))
        )
        .withColumn(
            "quarantined_at",
            when(
                col("quarantine_reason").isNotNull(),
                current_timestamp()
            )
        )
        .drop("_reason_parts")
    )

    # 3. Separar registros rechazados y registros válidos
    df_business_quarantine = df_evaluated.filter(
        col("quarantine_reason").isNotNull()
    )

    df_sanitized = df_evaluated.filter(
        col("quarantine_reason").isNull()
    )

    # 4. Consolidar cuarentena estructural y de negocio

    if df_structural_quarantine is not None:

        # Estandarizar los nombres de columnas de la cuarentena estructural
        for col_name in df_structural_quarantine.columns:
            if col_name != "_corrupt_record":
                df_structural_quarantine = (
                    df_structural_quarantine.withColumnRenamed(
                        col_name,
                        to_snake_case(col_name)
                    )
                )

        df_final_quarantine = df_structural_quarantine.unionByName(
            df_business_quarantine,
            allowMissingColumns=True
        )

    else:
        df_final_quarantine = df_business_quarantine

    # Contabilizar registros en cuarentena
    total_quarantine = df_final_quarantine.count()

    # Persistir registros rechazados en MinIO
    if total_quarantine > 0:
        print(
            f"Enviando {total_quarantine} registros a cuarentena..."
        )

        df_final_quarantine.write \
            .mode("overwrite") \
            .parquet(quarantine_path)

        print(f"Cuarentena guardada en: {quarantine_path}")
    else:
        print("No se encontraron registros para cuarentena.")

    # MÉTRICAS Y VALIDACIÓN NATIVA SEGURA
    total_filas = df_sanitized.count()
    print(f"\n[MÉTRICAS] Total de registros listos para Silver: {total_filas}")

    # Evaluar cuántos registros incumplen cada regla
    resultados_reglas = df_clean.agg(
        *[
            _sum(
                when(condicion, 1).otherwise(0)
            ).alias(nombre)
            for nombre, condicion in reglas.items()
        ]
    ).first().asDict()

    # Número real de reglas ejecutadas
    reglas_evaluadas = len(reglas)

    # Número de reglas que tuvieron al menos un incumplimiento
    reglas_fallidas = sum(
        1
        for cantidad in resultados_reglas.values()
        if (cantidad or 0) > 0
    )

    # Detalle de incumplimientos
    detalle_reglas = "\n".join(
        f"{nombre}: {cantidad or 0} registros afectados"
        for nombre, cantidad in resultados_reglas.items()
    )

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

    Detalle de incumplimientos:
    {detalle_reglas}
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
    print(
        f"Proceso finalizado. "
        f"Válidos: {total_filas}, "
        f"En cuarentena: {total_quarantine}, "
        f"Reglas fallidas: {reglas_fallidas}"
    )    
    # Escritura controlada con coalesce para evitar desbordamiento en JVM
    df_sanitized.drop(
    "quarantine_reason",
    "quarantined_at"
    ).coalesce(2).write.mode("overwrite").parquet(silver_path)
    
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

    df.unpersist()            
    spark.stop()

if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Procesamiento Bronze a Silver Dataset 1 con control de Esquema y Cuarentena")
    parser.add_argument("--run_id", required=True, help="Identificador único de ejecución (Run ID de Airflow)")
    args = parser.parse_args()
    
    process_transactions(args.run_id)