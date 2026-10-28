from airflow import DAG
from airflow.operators.bash import BashOperator
from datetime import datetime, timedelta
import logging

# FUNCIÓN DE ALERTA (Tolerancia a fallos global)
def task_failure_alert(context):
    """
    Función que se ejecuta automáticamente cuando una tarea agota todos sus reintentos.
    Genera un log crítico estructurado para rápida intervención en el DAG maestro.
    """
    task_instance = context.get('task_instance')
    
    alerta_msg = f"""
    ====================================================
    ALERTA CRÍTICA: FALLO EN DAG MAESTRO MEDALLION
    ====================================================
    DAG: {task_instance.dag_id}
    Tarea Fallida: {task_instance.task_id}
    Fecha de Ejecución: {context.get('execution_date')}
    Intentos agotados: {task_instance.try_number - 1}
    Log URL: {task_instance.log_url}
    ====================================================
    Acción requerida: Intervención manual o revisión de dependencias.
    """
    logging.error(alerta_msg)

default_args = {
    'owner': 'gemelo_admin',
    'depends_on_past': False,
    'start_date': datetime(2026, 8, 30),
    'retries': 0,                           
    'retry_delay': timedelta(minutes=5),    
    'on_failure_callback': task_failure_alert, 
}

with DAG(
    'dag_maestro_medallion',
    default_args=default_args,
    description='Pipeline maestro end-to-end (Bronze -> Silver -> Gold) para el Gemelo Digital Financiero',
    schedule_interval='@daily',
    catchup=False,
    tags=['medallion', 'maestro', 'gemelo_financiero'],
) as dag:

    # ==========================================
    # 1. CAPA BRONZE (Ingesta cruda de datasets)
    # ==========================================
    ingesta_ds1 = BashOperator(
        task_id='ingesta_bronze_dataset1',
        bash_command='python /opt/airflow/src/ingestion/ingest_dataset_pyspark.py --input /opt/airflow/data/raw/Dataset1.csv --dataset Dataset1'
    )

    ingesta_ds2 = BashOperator(
        task_id='ingesta_bronze_dataset2',
        bash_command='python /opt/airflow/src/ingestion/ingest_dataset_pyspark.py --input /opt/airflow/data/raw/Dataset2.csv --dataset Dataset2'
    )

    ingesta_ds3 = BashOperator(
        task_id='ingesta_bronze_dataset3',
        bash_command='python /opt/airflow/src/ingestion/ingest_dataset_pyspark.py --input /opt/airflow/data/raw/Dataset3.csv --dataset Dataset3'
    )

    # ==========================================
    # 2. CAPA SILVER (Limpieza y estandarización)
    # ==========================================
    transformacion_silver_ds1 = BashOperator(
        task_id='transformacion_silver_dataset1',
        bash_command='python /opt/airflow/src/transformation/bronze_to_silver_dataset1_pyspark.py --run_id "{{ run_id }}"'
    )

    transformacion_silver_ds2 = BashOperator(
        task_id='transformacion_silver_dataset2',
        bash_command='python /opt/airflow/src/transformation/bronze_to_silver_dataset2_pyspark.py --run_id "{{ run_id }}"'
    )

    transformacion_silver_ds3 = BashOperator(
        task_id='transformacion_silver_dataset3',
        bash_command='python /opt/airflow/src/transformation/bronze_to_silver_dataset3_pyspark.py --run_id "{{ run_id }}"'
    )

    # ==========================================
    # 3. CAPA GOLD (Modelado analítico y ML)
    # ==========================================
    run_gold_dataset1 = BashOperator(
        task_id='run_silver_to_gold_dataset1',
        bash_command='python /opt/airflow/src/transformation/silver_to_gold_dataset1.py --run_id "{{ run_id }}"',
    )

    run_gold_dataset2 = BashOperator(
        task_id='run_silver_to_gold_dataset2',
        bash_command='python /opt/airflow/src/transformation/silver_to_gold_dataset2.py --run_id "{{ run_id }}"',
    )

    run_gold_dataset3 = BashOperator(
        task_id='run_silver_to_gold_dataset3',
        bash_command='python /opt/airflow/src/transformation/silver_to_gold_dataset3.py --run_id "{{ run_id }}"',
    )

    # ==========================================
    # 4. DEFINICIÓN DE DEPENDENCIAS (FLUJO)
    # ==========================================
    # Dataset 1: Bronze -> Silver -> Gold
    ingesta_ds1 >> transformacion_silver_ds1 >> run_gold_dataset1

    # Dataset 2: Bronze -> Silver -> Gold
    ingesta_ds2 >> transformacion_silver_ds2 >> run_gold_dataset2

    # Dataset 3: Bronze -> Silver -> Gold
    ingesta_ds3 >> transformacion_silver_ds3 >> run_gold_dataset3