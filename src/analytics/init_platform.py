"""Inicialización repetible de esquemas PostgreSQL, roles y buckets MinIO."""
from minio import Minio
from psycopg2 import sql

from src.common.config import ROOT, db_connect, required


def initialize():
    with db_connect() as conn, conn.cursor() as cursor:
        cursor.execute((ROOT / "sql" / "platform.sql").read_text())
        role = required("GOLD_READER_USER")
        if role == required("POSTGRES_USER"):
            raise ValueError("GOLD_READER_USER debe ser distinto de POSTGRES_USER")
        cursor.execute("SELECT 1 FROM pg_roles WHERE rolname=%s", (role,))
        if cursor.fetchone() is None:
            cursor.execute(sql.SQL("CREATE ROLE {} LOGIN").format(sql.Identifier(role)))
        cursor.execute(sql.SQL("ALTER ROLE {} WITH LOGIN NOSUPERUSER NOCREATEDB NOCREATEROLE PASSWORD %s").format(sql.Identifier(role)), (required("GOLD_READER_PASSWORD"),))
        cursor.execute(sql.SQL("GRANT USAGE ON SCHEMA serving, simulation TO {}").format(sql.Identifier(role)))
        cursor.execute(sql.SQL("GRANT SELECT ON ALL TABLES IN SCHEMA serving, simulation TO {}").format(sql.Identifier(role)))
        cursor.execute(sql.SQL("ALTER DEFAULT PRIVILEGES IN SCHEMA serving, simulation GRANT SELECT ON TABLES TO {}").format(sql.Identifier(role)))
    endpoint = required("MINIO_ENDPOINT")
    client = Minio(endpoint.removeprefix("https://").removeprefix("http://"),
                   access_key=required("MINIO_ROOT_USER"), secret_key=required("MINIO_ROOT_PASSWORD"),
                   secure=endpoint.startswith("https://"))
    for bucket in ("bronze", "silver", "gold", "quarantine", "simulations", "models"):
        if not client.bucket_exists(bucket):
            client.make_bucket(bucket)
    print("Esquemas, rol lector y buckets preparados")


if __name__ == "__main__":
    initialize()
