from datetime import datetime
from airflow import DAG
from airflow.operators.bash import BashOperator

default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
}

with DAG(
    'gold_financial_profile_pipeline',
    default_args=default_args,
    description='Pipeline de Silver a Gold para Dataset 3 - Financial Profile',
    schedule_interval=None,
    start_date=datetime(2026, 1, 1),
    catchup=False,
    tags=['gold', 'financial_profile', 'gemelo_financiero'],
) as dag:

    run_gold_transformation = BashOperator(
        task_id='run_silver_to_gold_dataset3',
        bash_command='python /opt/airflow/src/transformation/silver_to_gold_dataset3.py --run_id {{ run_id }}',
    )

    run_gold_transformation