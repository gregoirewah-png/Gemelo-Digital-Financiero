"""Eventos idempotentes por tarea/intento, compartidos con Airflow y Streamlit."""
import logging
import os
import time
from contextlib import contextmanager

from psycopg2.extras import Json

from src.common.config import db_connect, redact

_metrics = {}


def record_metrics(**values):
    _metrics.update(values)


@contextmanager
def tracked_job(run_id, task_id, dataset, layer):
    _metrics.clear()
    if os.getenv("PIPELINE_AIRFLOW_LOG_URL"):
        _metrics["airflow_log_url"] = os.environ["PIPELINE_AIRFLOW_LOG_URL"]
    started = time.monotonic()
    attempt = int(os.getenv("PIPELINE_ATTEMPT", "1"))
    with db_connect() as conn, conn.cursor() as cursor:
        cursor.execute("""
            INSERT INTO ops.job_runs(run_id, task_id, attempt, dataset, layer, status, started_at)
            VALUES (%s,%s,%s,%s,%s,'RUNNING',now())
            ON CONFLICT (run_id,task_id,attempt) DO UPDATE
            SET status='RUNNING', started_at=now(), finished_at=NULL, metrics='{}', error=NULL
        """, (run_id, task_id, attempt, dataset, layer))
    status, error = "SUCCESS", None
    try:
        yield
    except BaseException as exc:
        status, error = "FAILED", redact(str(exc))[:4000]
        logging.exception("Falló %s", task_id)
        raise
    finally:
        log_path = os.getenv("PIPELINE_TASK_LOG", "")
        content = ""
        if log_path:
            from pathlib import Path
            content = redact(Path(log_path).read_text(errors="replace")[-60000:])
        with db_connect() as conn, conn.cursor() as cursor:
            cursor.execute("""
                UPDATE ops.job_runs SET status=%s, finished_at=now(), duration_seconds=%s,
                    metrics=%s, error=%s, log_content=%s
                WHERE run_id=%s AND task_id=%s AND attempt=%s
            """, (status, time.monotonic() - started, Json(_metrics), error,
                  content, run_id, task_id, attempt))
