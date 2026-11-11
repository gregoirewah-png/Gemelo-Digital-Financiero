"""Comprobaciones sobre la plataforma activa. Ejecutar como módulo tras publicar Gold."""
import hashlib
import json

from psycopg2 import errors

from src.analytics.run_monte_carlo import run_simulation
from src.common.config import db_connect
from src.ml.agent_service import query_gold
from src.ml.monte_carlo import MonteCarloConfig


def snapshot_hash():
    with db_connect(reader=True) as conn, conn.cursor() as cursor:
        cursor.execute('SELECT * FROM serving.monthly_cashflow ORDER BY period_month')
        return hashlib.sha256(json.dumps(cursor.fetchall(),default=str).encode()).hexdigest()


def check():
    monthly=query_gold('dataset1','monthly')
    assert len(monthly['rows'])==60
    assert query_gold('dataset2')['rows']
    assert query_gold('dataset3')['rows'][0]['profiles']>0
    before=snapshot_hash()
    config=MonteCarloConfig(initial_savings=10000,months=12,simulations=1000,seed=17)
    result=run_simulation(configuration=config)
    assert len(result['results'])==36
    assert snapshot_hash()==before
    retry=run_simulation(source_run_id=result['source_run_id'],simulation_run_id=result['simulation_run_id'],configuration=config)
    assert result['results']==retry['results']
    try:
        run_simulation(source_run_id=result['source_run_id'],simulation_run_id=result['simulation_run_id'],configuration=MonteCarloConfig(seed=18))
        raise AssertionError('Se permitió cambiar una simulación ya publicada')
    except ValueError:
        pass
    try:
        with db_connect(reader=True) as conn, conn.cursor() as cursor:
            cursor.execute("INSERT INTO simulation.runs(simulation_run_id) VALUES ('unauthorized')")
        raise AssertionError('El rol lector pudo escribir')
    except (errors.ReadOnlySqlTransaction,errors.InsufficientPrivilege):
        pass
    print(json.dumps({'publication':monthly['publication'],'gold_hash_unchanged':before,
                      'simulation_run_id':result['simulation_run_id'],
                      'checks':['3 fuentes accesibles','historia inmutable','simulación reproducible',
                                'identificador inmutable','rol de consulta de solo lectura']},default=str,ensure_ascii=False))


if __name__=='__main__':
    check()
