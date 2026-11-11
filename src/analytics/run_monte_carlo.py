"""Simulaciones inmutables sobre la historia Gold original o una persona identificada."""
import io
import json
import os
import uuid
from dataclasses import asdict

import pandas as pd
from minio import Minio
from psycopg2.extras import Json, RealDictCursor, execute_values

from src.common.config import db_connect, required, safe_run_id
from src.common.observability import record_metrics
from src.ml.monte_carlo import MonteCarloConfig, simulate


def run_simulation(source_run_id=None, simulation_run_id=None, configuration=None, profile_id=None):
    profile=None
    with db_connect() as conn, conn.cursor(cursor_factory=RealDictCursor) as cursor:
        if source_run_id is None:
            cursor.execute('SELECT run_id FROM serving.publication')
            publication=cursor.fetchone()
            if not publication:
                raise ValueError('Todavía no hay una publicación disponible')
            source_run_id=publication['run_id']
        if profile_id:
            cursor.execute('SELECT payload FROM gold.personal_profiles WHERE source_run_id=%s AND profile_id=%s',
                           (source_run_id,profile_id))
            selected=cursor.fetchone()
            if not selected:
                raise ValueError('No hay un perfil publicado con esa identidad')
            profile=selected['payload']
            cursor.execute('SELECT period_month,total_income,total_expenses FROM gold.personal_monthly_cashflow '
                           'WHERE source_run_id=%s AND profile_id=%s ORDER BY period_month',(source_run_id,profile_id))
        else:
            cursor.execute('SELECT period_month,total_income,total_expenses FROM gold.monthly_cashflow '
                           'WHERE source_run_id=%s ORDER BY period_month',(source_run_id,))
        history=pd.DataFrame(cursor.fetchall(),columns=['period_month','total_income','total_expenses'])
    config=configuration or MonteCarloConfig(
        initial_savings=float(profile['available_savings']) if profile else float(os.getenv('MC_INITIAL_SAVINGS','0')),
        months=int(os.getenv('MC_MONTHS','12')),simulations=int(os.getenv('MC_SIMULATIONS','5000')),
        seed=int(os.getenv('MC_SEED','42')))
    config.validate()
    simulation_run_id=simulation_run_id or 'mc_'+uuid.uuid4().hex
    result,metadata=simulate(history,config)
    if profile:
        metadata.update(profile_id=profile_id,currency=profile['currency'],display_name=profile['display_name'])
    key=f'personal/{safe_run_id(simulation_run_id)}' if profile_id else f'dataset1/{safe_run_id(simulation_run_id)}'
    destination=f's3://simulations/{key}'
    with db_connect() as conn, conn.cursor() as cursor:
        cursor.execute('SELECT pg_advisory_xact_lock(hashtextextended(%s,0))',(simulation_run_id,))
        cursor.execute('SELECT source_run_id,configuration,history_hash,profile_id,model_version FROM simulation.runs '
                       'WHERE simulation_run_id=%s',(simulation_run_id,))
        previous=cursor.fetchone()
        if previous:
            if previous[:4]!=(source_run_id,asdict(config),metadata['history_hash'],profile_id):
                raise ValueError('Ese identificador ya existe con otra configuración o historia')
            metadata['model_version']=previous[4]
            with conn.cursor(cursor_factory=RealDictCursor) as results_cursor:
                results_cursor.execute('SELECT scenario,month,period_month,p05,p50,p95,mean_balance,probability_negative,'
                                       'probability_ever_negative FROM simulation.results WHERE simulation_run_id=%s',
                                       (simulation_run_id,))
                stored=[dict(row) for row in results_cursor.fetchall()]
            order={scenario['name']:index for index,scenario in enumerate(config.scenarios)}
            stored.sort(key=lambda row:(order[row['scenario']],row['month']))
            return {'simulation_run_id':simulation_run_id,'source_run_id':source_run_id,'metadata':metadata,'results':stored}
        endpoint=required('MINIO_ENDPOINT')
        client=Minio(endpoint.split('://',1)[-1],access_key=required('MINIO_ROOT_USER'),
                     secret_key=required('MINIO_ROOT_PASSWORD'),secure=endpoint.startswith('https://'))
        for filename,payload in [('results.json',result.to_json(orient='records',date_format='iso').encode()),
                                 ('metadata.json',json.dumps({**metadata,'simulation_run_id':simulation_run_id,
                                                             'source_run_id':source_run_id},ensure_ascii=False).encode())]:
            client.put_object('simulations',f'{key}/{filename}',io.BytesIO(payload),len(payload),content_type='application/json')
        cursor.execute('INSERT INTO simulation.runs(simulation_run_id,source_run_id,configuration,historical_months,'
                       'history_hash,model_version,output_path,profile_id) VALUES (%s,%s,%s,%s,%s,%s,%s,%s)',
                       (simulation_run_id,source_run_id,Json(asdict(config)),metadata['historical_months'],
                        metadata['history_hash'],metadata['model_version'],destination,profile_id))
        columns=list(result.columns)
        execute_values(cursor,'INSERT INTO simulation.results(simulation_run_id,'+','.join(columns)+') VALUES %s',
                       [(simulation_run_id,*[row[column] for column in columns]) for row in result.to_dict('records')])
    record_metrics(records_read=len(history),records_written=len(result),simulation_run_id=simulation_run_id)
    return {'simulation_run_id':simulation_run_id,'source_run_id':source_run_id,'metadata':metadata,'results':result.to_dict('records')}
