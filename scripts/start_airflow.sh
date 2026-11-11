#!/usr/bin/env bash
set -euo pipefail
# Construir la URL con escape correcto de credenciales y mantenerla fuera del YAML.
AIRFLOW__DATABASE__SQL_ALCHEMY_CONN="$(python -c 'from src.common.config import airflow_database_url; print(airflow_database_url())')"
export AIRFLOW__DATABASE__SQL_ALCHEMY_CONN
airflow db migrate
airflow users create --username "$AIRFLOW_ADMIN_USER" --password "$AIRFLOW_ADMIN_PASSWORD" \
    --firstname "$AIRFLOW_ADMIN_FIRSTNAME" --lastname "$AIRFLOW_ADMIN_LASTNAME" \
    --role "$AIRFLOW_ADMIN_ROLE" --email "$AIRFLOW_ADMIN_EMAIL"
airflow users reset-password --username "$AIRFLOW_ADMIN_USER" --password "$AIRFLOW_ADMIN_PASSWORD"
exec airflow standalone
