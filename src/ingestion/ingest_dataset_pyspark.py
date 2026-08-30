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
BRONZE_DIR = Path("data/bronze")


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

    # Evitar duplicar handlers si el script se ejecuta
    # dentro del mismo proceso.
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

    spark = (
        SparkSession.builder
        .appName("financial_twin_ingestion")
        .master("local[*]")
        .getOrCreate()
    )

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

    bronze_path = (
        BRONZE_DIR /
        dataset_name /
        f"ingestion_date={ingestion_date}"
    )

    metadata_path = bronze_path / "metadata.json"

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
            "Validando existencia del archivo..."
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
            "Creando SparkSession..."
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
        # 5. Crear Bronze
        # ----------------------------------------------------

        bronze_path.mkdir(
            parents=True,
            exist_ok=True
        )

        logging.info(
            f"Guardando dataset en Bronze: "
            f"{bronze_path}"
        )

        (
            df.write
            .mode("overwrite")
            .option("header", "true")
            .csv(str(bronze_path))
        )

        logging.info(
            "Dataset almacenado correctamente."
        )

        # ----------------------------------------------------
        # 6. Metadata
        # ----------------------------------------------------

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
            metadata_path,
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
            f"Metadata generada: {metadata_path}"
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

        # Si el error ocurre después de conocer
        # la ruta Bronze, registrar metadata de fallo.
        bronze_path.mkdir(
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
            metadata_path,
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


# CLI

def main():

    parser = argparse.ArgumentParser(
        description=(
            "Pipeline genérico de ingesta "
            "de datasets financieros hacia Bronze"
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