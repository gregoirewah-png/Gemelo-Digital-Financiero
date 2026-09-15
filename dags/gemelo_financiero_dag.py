from airflow import DAG
from airflow.operators.bash import BashOperator
from datetime import datetime, timedelta
import logging

# FUNCIÓN DE ALERTA (Tolerancia a fallos)
def task_failure_alert(context):
    """
    Función que se ejecuta automáticamente cuando una tarea agota todos sus reintentos.
    Genera un log crítico estructurado para rápida intervención.
    """
    task_instance = context.get('task_instance')
    
    alerta_msg = f"""
    ====================================================
    ALERTA CRÍTICA: FALLO EN PIPELINE DE DATOS
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
    # Políticas de Tolerancia a Fallos
    'retries': 3,                           
    'retry_delay': timedelta(minutes=2),    
    'on_failure_callback': task_failure_alert, 
}

with DAG(
    'ingesta_diaria_medallion',
    default_args=default_args,
    description='Pipeline batch diario para el Gemelo Digital Financiero',
    schedule_interval='@daily',
    catchup=False,
) as dag:

    # 1. Tareas de Ingesta a Bronze
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

    # 2. Tareas de Transformación a Silver (Limpieza y GE)
    transformacion_ds1 = BashOperator(
        task_id='transformacion_silver_dataset1',
        bash_command='pip install "great_expectations<1.0.0" && python /opt/airflow/src/transformation/bronze_to_silver_dataset1_pyspark.py'
    )

    transformacion_ds2 = BashOperator(
        task_id='transformacion_silver_dataset2',
        bash_command='pip install "great_expectations<1.0.0" && python /opt/airflow/src/transformation/bronze_to_silver_dataset2_pyspark.py'
    )

    transformacion_ds3 = BashOperator(
        task_id='transformacion_silver_dataset3',
        bash_command='pip install "great_expectations<1.0.0" && python /opt/airflow/src/transformation/bronze_to_silver_dataset3_pyspark.py'
    )

    # 3. Definición de dependencias
    ingesta_ds1 >> transformacion_ds1
    ingesta_ds2 >> transformacion_ds2
    ingesta_ds3 >> transformacion_ds3