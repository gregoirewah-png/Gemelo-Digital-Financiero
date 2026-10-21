from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.bash import BashOperator

default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

with DAG(
    'gold_dataset1_pipeline',
    default_args=default_args,
    description='DAG para procesar la Capa Gold del Dataset 1 (Salud Financiera)',
    schedule_interval='@daily',
    start_date=datetime(2026, 1, 1),
    catchup=False,
    tags=['medallion', 'gold', 'dataset_1'],
) as dag:

    # Tarea que ejecuta el script de PySpark pasando el Run ID de Airflow
    run_gold_dataset1 = BashOperator(
        task_id='run_silver_to_gold_dataset1',
        bash_command=(
            'python /opt/airflow/src/transformation/silver_to_gold_dataset1.py '
            '--run_id {{ run_id }}'
        ),
    )

    run_gold_dataset1