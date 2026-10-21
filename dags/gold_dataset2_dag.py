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
    'gold_dataset2_pipeline',
    default_args=default_args,
    description='DAG para procesar la Capa Gold del Dataset 2',
    schedule_interval='@daily',
    start_date=datetime(2026, 1, 1),
    catchup=False,
    tags=['medallion', 'gold', 'dataset_2'],
) as dag:

    run_gold_dataset2 = BashOperator(
        task_id='run_silver_to_gold_dataset2',
        bash_command=(
            'python /opt/airflow/src/transformation/silver_to_gold_dataset2.py '
            '--run_id {{ run_id }}'
        ),
    )

    run_gold_dataset2