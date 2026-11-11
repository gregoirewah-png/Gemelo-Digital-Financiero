"""Configuración compartida. Las credenciales se leen exclusivamente del entorno."""
import os
import re
from contextlib import contextmanager
from pathlib import Path

from dotenv import load_dotenv

ROOT = Path(__file__).resolve().parents[2]
load_dotenv(ROOT / ".env", override=False, interpolate=False)


def required(name):
    value = os.getenv(name)
    if not value:
        raise RuntimeError(f"Falta configurar {name} en .env")
    return value


def airflow_database_url():
    from urllib.parse import quote
    username = quote(required("POSTGRES_USER"), safe="")
    password = quote(required("POSTGRES_PASSWORD"), safe="")
    database = quote(required("POSTGRES_DB"), safe="")
    return (f"postgresql+psycopg2://{username}:{password}"
            f"@{required('POSTGRES_HOST')}:{required('POSTGRES_PORT')}/{database}")


def safe_run_id(run_id):
    if not run_id or len(run_id) > 250:
        raise ValueError("run_id debe contener entre 1 y 250 caracteres")
    # Hash evita colisiones entre IDs que se normalizan al mismo texto.
    import hashlib
    slug = re.sub(r"[^A-Za-z0-9_.=-]", "_", run_id)
    return slug + "_" + hashlib.sha256(run_id.encode()).hexdigest()[:12]


def lake_path(layer, dataset, run_id):
    return f"s3a://{layer}/{dataset}/run_id={safe_run_id(run_id)}"


@contextmanager
def db_connect(reader=False):
    import psycopg2
    prefix = "GOLD_READER" if reader else "POSTGRES"
    connection = psycopg2.connect(
        host=required("POSTGRES_HOST"),
        port=required("POSTGRES_PORT"),
        dbname=required("POSTGRES_DB"),
        user=required(f"{prefix}_USER"),
        password=required(f"{prefix}_PASSWORD"),
        connect_timeout=10,
        application_name="gemelo_reader" if reader else "gemelo_pipeline",
    )
    if reader:
        connection.set_session(readonly=True)
    try:
        with connection:
            yield connection
    finally:
        connection.close()


def redact(text):
    for name, value in os.environ.items():
        if any(token in name for token in ("PASSWORD", "SECRET", "API_KEY", "FERNET")) and value:
            text = text.replace(value, "[REDACTED]")
    return text
