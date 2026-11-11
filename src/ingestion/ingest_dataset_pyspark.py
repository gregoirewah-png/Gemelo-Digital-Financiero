"""Ingesta auditable. Conserva los bytes originales y aísla cada corrida."""
import argparse
import hashlib
import json
import os
from datetime import datetime, timezone
from pathlib import Path

from minio import Minio

import sys
from pathlib import Path
sys.path.insert(0, str(Path(__file__).resolve().parents[2]))

from src.common.config import lake_path, required
from src.common.observability import record_metrics
from src.common.spark import spark_session


def ingest_dataset(input_file, dataset_name, run_id):
    source = Path(input_file)
    if not source.is_file():
        raise FileNotFoundError(f"No existe el archivo {source}")
    endpoint = required("MINIO_ENDPOINT")
    client = Minio(endpoint.split("://", 1)[-1], access_key=required("MINIO_ROOT_USER"),
                   secret_key=required("MINIO_ROOT_PASSWORD"), secure=endpoint.startswith("https://"))
    destination = lake_path("bronze", dataset_name, run_id)
    prefix = destination.removeprefix("s3a://bronze/")
    digest = hashlib.sha256(source.read_bytes()).hexdigest()
    # Reintentar una corrida exige el mismo contenido, para preservar su linaje.
    from minio.error import S3Error
    metadata_key = f"{prefix}/metadata.json"
    previous = None
    try:
        response = client.get_object("bronze", metadata_key)
        try:
            previous = json.loads(response.read())
        finally:
            response.close()
            response.release_conn()
    except S3Error as exc:
        if exc.code != "NoSuchKey":
            raise
    if previous and previous["sha256"] != digest:
        raise ValueError("La fuente cambió para este run_id. Genera una corrida nueva.")
    spark = spark_session("Bronze_" + dataset_name)
    try:
        raw = spark.read.option("header", True).option("inferSchema", False).csv(str(source))
        count = raw.count()
        if count == 0:
            raise ValueError("La fuente no contiene registros")
        client.fput_object("bronze", f"{prefix}/source.csv", str(source), content_type="text/csv")
        metadata = {"run_id": run_id, "dataset": dataset_name, "records": count,
                    "columns": raw.columns, "sha256": digest,
                    "ingested_at": datetime.now(timezone.utc).isoformat()}
        import io
        payload = json.dumps(metadata).encode()
        client.put_object("bronze", metadata_key, io.BytesIO(payload), len(payload), content_type="application/json")
        record_metrics(records_read=count, records_written=count, source_sha256=digest, output_path=destination)
        print(f"Bronze {dataset_name}. Registros {count}. SHA256 {digest}. Destino {destination}")
    finally:
        spark.stop()


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("--input", required=True)
    parser.add_argument("--dataset", required=True)
    parser.add_argument("--run-id", "--run_id", dest="run_id", required=True)
    args = parser.parse_args()
    ingest_dataset(args.input, args.dataset, args.run_id)
