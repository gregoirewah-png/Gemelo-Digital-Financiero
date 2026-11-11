"""Malla completa. Gold solo se expone cuando sus tres fuentes y ML están validados."""
import logging
from datetime import datetime, timedelta, timezone

from airflow import DAG
from airflow.operators.bash import BashOperator
from src.common.config import required


def task_failure_alert(context):
    task = context['task_instance']
    logging.error('Fallo de tarea dag=%s task=%s run=%s intento=%s log=%s',
                  task.dag_id,task.task_id,context['run_id'],task.try_number,task.log_url)
    from src.common.config import db_connect
    with db_connect() as conn, conn.cursor() as cursor:
        cursor.execute("UPDATE ops.job_runs SET status='FAILED',finished_at=now(),error=%s "
                       "WHERE run_id=%s AND task_id=%s AND status='RUNNING'",
                       ('Airflow agotó los intentos o terminó la tarea. Consulta su log.',context['run_id'],task.task_id))


with DAG('dag_maestro_medallion',
         start_date=datetime(2026,8,30,tzinfo=timezone.utc),
         schedule='@daily',catchup=False,max_active_runs=1,max_active_tasks=1,
         default_args={'owner':required('AIRFLOW_DAG_OWNER'),'retries':2,'retry_delay':timedelta(minutes=1),
                       'execution_timeout':timedelta(minutes=30),'on_failure_callback':task_failure_alert},
         tags=['medallion','gemelo_financiero']) as dag:
    initialize = BashOperator(task_id='preparar_plataforma',
                              bash_command='python -m src.analytics.init_platform',cwd='/opt/airflow/project')
    tasks = {}
    for job in ['bronze1','bronze2','bronze3','silver1','silver2','silver3','gold1','gold2','gold3','anomalies','publish','monte_carlo','risk_ingestion','risk_features','risk_model','demo_ingestion','personal_profile','personal_simulation']:
        tasks[job] = BashOperator(
            task_id=job,
            bash_command='python -m src.pipeline --job ' + job + ' --run-id "$PIPELINE_RUN_ID"',
            cwd='/opt/airflow/project',append_env=True,
            env={'PIPELINE_RUN_ID':'{{ run_id }}','PIPELINE_ATTEMPT':'{{ ti.try_number }}','PIPELINE_AIRFLOW_LOG_URL':'{{ ti.log_url }}'},
        )
    for index in (1,2,3):
        initialize >> tasks[f'bronze{index}'] >> tasks[f'silver{index}'] >> tasks[f'gold{index}']
    tasks['gold2'] >> tasks['anomalies']
    initialize >> tasks['risk_ingestion'] >> tasks['risk_features'] >> tasks['risk_model']
    initialize >> tasks['demo_ingestion']
    [tasks['risk_model'],tasks['demo_ingestion']] >> tasks['personal_profile']
    [tasks['gold1'],tasks['gold3'],tasks['anomalies'],tasks['personal_profile']] >> tasks['publish']
    tasks['publish'] >> tasks['monte_carlo']
    tasks['publish'] >> tasks['personal_simulation']


# Recalcular sobre Gold publicado sin ejecutar ni sobrescribir la malla histórica.
with DAG('recalcular_monte_carlo',start_date=datetime(2026,8,30,tzinfo=timezone.utc),
         schedule=None,catchup=False,max_active_runs=1,
         default_args={'owner':required('AIRFLOW_DAG_OWNER'),'retries':1,'retry_delay':timedelta(minutes=1)},
         tags=['monte_carlo']) as monte_carlo_dag:
    BashOperator(task_id='simular_gold_publicado',cwd='/opt/airflow/project',append_env=True,
                 bash_command='python -m src.analytics.recalculate_simulation',
                 env={'SIMULATION_CONFIG':'{{ dag_run.conf | tojson }}','SIMULATION_RUN_ID':'{{ run_id }}'})
