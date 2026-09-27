import argparse
import json
import logging
import os
import uuid
from datetime import datetime
from pathlib import Path

from pyspark.sql import SparkSession


# CONFIGURACIÓN

LOG_DIR = Path("logs")
# Carpeta local para almacenar los metadatos de ejecución
LOCAL_METADATA_DIR = Path("data/metadata/bronze")


# LOGGING

def configure_logging(dataset_name: str):

    LOG_DIR.mkdir(
        parents=True,
        exist_ok=True
    )

    log_file = (
        LOG_DIR /
        f"ingest_{dataset_name}.log"
    )

    logger = logging.getLogger()

    logger.setLevel(logging.INFO)

    # Evitar duplicar handlers si el script se ejecuta dentro del mismo proceso.
    if logger.handlers:
        logger.handlers.clear()

    formatter = logging.Formatter(
        "%(asctime)s | %(levelname)s | %(message)s"
    )

    file_handler = logging.FileHandler(
        log_file,
        encoding="utf-8"
    )

    console_handler = logging.StreamHandler()

    file_handler.setFormatter(formatter)
    console_handler.setFormatter(formatter)

    logger.addHandler(file_handler)
    logger.addHandler(console_handler)

    return log_file


# SPARK

def create_spark_session():
    # 1. Parámetros nivel JVM para evadir configuraciones defectuosas de Hadoop
    jvm_options = (
        "-Dfs.s3a.threads.keepalivetime=60 "
        "-Dfs.s3a.connection.timeout=600000 "
        "-Dfs.s3a.connection.establish.timeout=600000 "
        "-Dfs.s3a.multipart.purge.age=86400"
    )

    # 2. Inicialización de la sesión
    spark = (
        SparkSession.builder
        .appName("financial_twin_ingestion")
        .config("spark.jars.packages", "org.apache.hadoop:hadoop-aws:3.3.4,com.amazonaws:aws-java-sdk-bundle:1.12.262")
        .config("spark.driver.extraJavaOptions", jvm_options)
        .config("spark.hadoop.fs.s3a.endpoint", "http://gemelo-minio:9000")
        .config("spark.hadoop.fs.s3a.access.key", "minioadmin")
        .config("spark.hadoop.fs.s3a.secret.key", "minioadmin123")
        .config("spark.hadoop.fs.s3a.path.style.access", "true")
        .config("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem")
        .config("spark.hadoop.fs.s3a.aws.credentials.provider", "org.apache.hadoop.fs.s3a.SimpleAWSCredentialsProvider")
        .config("spark.hadoop.fs.s3a.connection.timeout", "600000")
        .config("spark.hadoop.fs.s3a.connection.establish.timeout", "600000")
        .config("spark.hadoop.fs.s3a.threads.keepalivetime", "60")
        .config("spark.hadoop.fs.s3a.multipart.purge", "false")
        .config("spark.hadoop.fs.s3a.multipart.purge.age", "86400")
        .master("local[*]")
        .getOrCreate()
    )

    # 3. Vacuna extra sobre el contexto de Hadoop activo
    hadoop_conf = spark.sparkContext._jsc.hadoopConfiguration()
    hadoop_conf.set("fs.s3a.endpoint", "http://gemelo-minio:9000")
    hadoop_conf.set("fs.s3a.access.key", "minioadmin")
    hadoop_conf.set("fs.s3a.secret.key", "minioadmin123")
    hadoop_conf.set("fs.s3a.path.style.access", "true")
    hadoop_conf.set("fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem")
    hadoop_conf.set("fs.s3a.aws.credentials.provider", "org.apache.hadoop.fs.s3a.SimpleAWSCredentialsProvider")
    hadoop_conf.setBoolean("fs.s3a.multipart.purge", False)
    hadoop_conf.setLong("fs.s3a.multipart.purge.age", 86400)
    hadoop_conf.setLong("fs.s3a.threads.keepalivetime", 60)
    hadoop_conf.setLong("fs.s3a.connection.timeout", 600000)
    hadoop_conf.setLong("fs.s3a.connection.establish.timeout", 600000)

    spark.sparkContext.setLogLevel("WARN")

    return spark
# METADATA

def create_metadata(
    dataset_name,
    input_file,
    ingestion_date,
    row_count,
    column_count,
    columns,
    status,
    execution_id
):

    return {
        "source_name": dataset_name,
        "ingestion_date": ingestion_date,
        "file_name": os.path.basename(input_file),
        "row_count": row_count,
        "column_count": column_count,
        "columns": columns,
        "status": status,
        "execution_id": execution_id
    }


# PIPELINE

def ingest_dataset(
    input_file: str,
    dataset_name: str
):

    execution_id = str(uuid.uuid4())

    ingestion_date = datetime.now().strftime(
        "%Y-%m-%d"
    )

    # Ruta de destino masivo en el Data Lake (MinIO)
    s3_bronze_path = f"s3a://bronze/{dataset_name}/ingestion_date={ingestion_date}"

    # Ruta local auxiliar para la metadata
    local_meta_path = (
        LOCAL_METADATA_DIR /
        dataset_name /
        f"ingestion_date={ingestion_date}"
    )
    metadata_file_path = local_meta_path / "metadata.json"

    log_file = configure_logging(
        dataset_name
    )

    logging.info("========================================")
    logging.info("INICIO DEL PIPELINE DE INGESTA")
    logging.info("========================================")

    logging.info(
        f"execution_id={execution_id}"
    )

    logging.info(
        f"dataset={dataset_name}"
    )

    logging.info(
        f"source={input_file}"
    )

    logging.info(
        f"ingestion_date={ingestion_date}"
    )

    spark = None

    try:

        # ----------------------------------------------------
        # 1. Validar archivo
        # ----------------------------------------------------

        logging.info(
            "Validando existencia del archivo crudo local..."
        )

        if not os.path.isfile(input_file):

            raise FileNotFoundError(
                f"No existe el archivo: {input_file}"
            )

        logging.info(
            "Archivo fuente encontrado correctamente."
        )

        # ----------------------------------------------------
        # 2. Crear SparkSession
        # ----------------------------------------------------

        logging.info(
            "Creando SparkSession (con soporte S3)..."
        )

        spark = create_spark_session()

        logging.info(
            "SparkSession creada correctamente."
        )

        # ----------------------------------------------------
        # 3. Leer CSV
        # ----------------------------------------------------

        logging.info(
            "Leyendo archivo CSV..."
        )

        df = (
            spark.read
            .option("header", "true")
            .option("inferSchema", "true")
            .option("mode", "PERMISSIVE")
            .csv(input_file)
        )

        # ----------------------------------------------------
        # 4. Estadísticas
        # ----------------------------------------------------

        row_count = df.count()

        column_count = len(
            df.columns
        )

        columns = df.columns

        logging.info(
            f"Registros detectados: {row_count}"
        )

        logging.info(
            f"Columnas detectadas: {column_count}"
        )

        logging.info(
            f"Columnas: {columns}"
        )

        # ----------------------------------------------------
        # 5. Crear Bronze (MinIO)
        # ----------------------------------------------------

        logging.info(
            f"Guardando dataset en Bronze (Data Lake): "
            f"{s3_bronze_path}"
        )

        (
            df.write
            .mode("overwrite")
            .option("header", "true")
            .csv(s3_bronze_path)
        )

        logging.info(
            "Dataset almacenado correctamente en MinIO."
        )

        # ----------------------------------------------------
        # 6. Metadata (Local)
        # ----------------------------------------------------

        local_meta_path.mkdir(
            parents=True,
            exist_ok=True
        )

        metadata = create_metadata(
            dataset_name=dataset_name,
            input_file=input_file,
            ingestion_date=ingestion_date,
            row_count=row_count,
            column_count=column_count,
            columns=columns,
            status="SUCCESS",
            execution_id=execution_id
        )

        with open(
            metadata_file_path,
            "w",
            encoding="utf-8"
        ) as metadata_file:

            json.dump(
                metadata,
                metadata_file,
                indent=4,
                ensure_ascii=False
            )

        logging.info(
            f"Metadata generada: {metadata_file_path}"
        )

        logging.info(
            f"Log generado: {log_file}"
        )

        logging.info(
            "========================================"
        )

        logging.info(
            "PIPELINE FINALIZADO CORRECTAMENTE"
        )

        logging.info(
            "========================================"
        )

    except Exception as error:

        logging.exception(
            f"ERROR EN PIPELINE: {error}"
        )

        # Si falla la ingesta, generar la metadata local de fallo
        local_meta_path.mkdir(
            parents=True,
            exist_ok=True
        )

        metadata = create_metadata(
            dataset_name=dataset_name,
            input_file=input_file,
            ingestion_date=ingestion_date,
            row_count=0,
            column_count=0,
            columns=[],
            status="FAILED",
            execution_id=execution_id
        )

        with open(
            metadata_file_path,
            "w",
            encoding="utf-8"
        ) as metadata_file:

            json.dump(
                metadata,
                metadata_file,
                indent=4,
                ensure_ascii=False
            )

        raise

    finally:

        if spark is not None:

            logging.info(
                "Deteniendo SparkSession..."
            )

            spark.stop()


def main():

    parser = argparse.ArgumentParser(
        description=(
            "Pipeline genérico de ingesta "
            "de datasets financieros hacia Bronze (MinIO)"
        )
    )

    parser.add_argument(
        "--input",
        required=True,
        help="Ruta del CSV de entrada"
    )

    parser.add_argument(
        "--dataset",
        required=True,
        help=(
            "Nombre lógico del dataset "
            "(ej. personal_transactions)"
        )
    )

    args = parser.parse_args()

    ingest_dataset(
        input_file=args.input,
        dataset_name=args.dataset
    )


if __name__ == "__main__":
    main()