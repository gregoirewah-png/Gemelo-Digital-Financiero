"""Publica una fotografía consistente de Gold para consumo SQL y valida su grano."""
import hashlib
import json
from decimal import Decimal

import pandas as pd
from psycopg2.extras import Json, execute_values

from src.common.config import db_connect, lake_path
from src.common.observability import record_metrics
from src.common.spark import spark_session, read_lake, write_lake
from src.analytics.publish_personal import validate_personal, insert_personal
from src.transformation.personal_profile import PERSONAL_TABLES

TABLES = {
    'monthly_cashflow': ('personal_transactions', ['period_month']),
    'category_spending': ('personal_transactions_category_spending', ['period_month', 'category']),
    'credit_cards': ('credit_card_balances', ['cust_id']),
    'financial_profiles': ('financial_profile', ['user_id', 'record_date']),
    'anomalies': ('credit_card_anomalies', ['cust_id']),
}


def validate_model(frames):
    checks = {}
    for name, (_, key) in TABLES.items():
        frame = frames[name]
        if frame[key].isna().any().any() or frame.duplicated(key).any():
            raise ValueError(f'{name} incumple su grano {key}')
        if frame.empty and name != 'category_spending':
            raise ValueError(f'{name} está vacío')
        checks[name] = {'grain': key, 'rows': len(frame), 'unique_key': True}
    monthly = frames['monthly_cashflow'].set_index('period_month')
    spending = frames['category_spending'].groupby('period_month')['expense_total'].sum()
    if not set(spending.index) <= set(monthly.index):
        raise ValueError('Categorías sin dimensión mensual')
    for month, row in monthly.iterrows():
        if abs(float(row.total_income) - float(row.total_expenses) - float(row.net_cash_flow)) > 0.011:
            raise ValueError('No concilia el flujo mensual')
        if abs(float(spending.get(month, 0)) - float(row.total_expenses)) > 0.011:
            raise ValueError('No concilian los gastos por categoría con el hecho mensual')
    cards = frames['credit_cards']
    anomalies = frames['anomalies']
    if set(cards.cust_id) != set(anomalies.cust_id):
        raise ValueError('Las features de anomalías no cubren exactamente los clientes de tarjetas')
    feature_columns = ['credit_utilization', 'payment_to_balance', 'minimum_payment_to_balance', 'is_anomaly', 'anomaly_score']
    if not set(feature_columns) <= set(anomalies.columns):
        raise ValueError('Faltan features de tarjetas')
    if anomalies.anomaly_score.isna().any() or not anomalies.anomaly_score.map(lambda x: abs(float(x)) < float('inf')).all():
        raise ValueError('Scores de anomalías inválidos')
    profiles = frames['financial_profiles']
    if not {'savings_capacity', 'emergency_fund_months', 'credit_score'} <= set(profiles.columns):
        raise ValueError('Faltan features del perfil')
    checks['relationships'] = {'month_category': 'valid', 'card_anomaly_1_to_1': 'valid',
                               'cross_dataset_identity': 'unavailable; fuentes independientes'}
    checks['feature_store'] = {'version': '1.0', 'card_key': ['source_run_id', 'cust_id'],
                               'profile_key': ['source_run_id', 'user_id', 'record_date'],
                               'available_at': 'gold.publications.published_at', 'point_in_time': 'filtrar available_at <= fecha de corte'}
    return checks


def canonical_record(record):
    return {key: None if pd.isna(value) else json.loads(json.dumps(value, default=str, ensure_ascii=False))
            for key, value in record.items()}


def publish(run_id):
    with db_connect() as conn, conn.cursor() as cursor:
        cursor.execute('SELECT manifest FROM gold.publications WHERE run_id=%s',(run_id,))
        existing=cursor.fetchone()
    extended = existing is None or 'personal_checks' in existing[0]
    personal = {}
    model_metrics = None
    spark = spark_session('PublishGoldSQL')
    frames = {}
    try:
        for name, (dataset, _) in TABLES.items():
            frame = read_lake(spark, lake_path('gold', dataset, run_id))
            if frame.count() > 100_000:
                raise ValueError(f'{dataset} excede el límite local de publicación de 100000 filas')
            frames[name] = frame.toPandas()
        if extended:
            for name in PERSONAL_TABLES:
                personal[name] = read_lake(spark,lake_path('gold',name,run_id)).toPandas()
            model_frame=read_lake(spark,lake_path('gold','credit_default_model',run_id))
            model_metrics=json.loads(model_frame.first()['metrics_json'])
    finally:
        spark.stop()
    checks = validate_model(frames)
    hashes = {}
    for name, frame in frames.items():
        key = TABLES[name][1]
        stable = frame.drop(columns=['processed_at', 'scored_at'], errors='ignore').sort_values(key)
        hashes[name] = hashlib.sha256(stable.to_json(orient='records', date_format='iso').encode()).hexdigest()
    personal_checks = validate_personal(personal,model_metrics) if extended else None
    if extended and (existing is None or 'credit_default_model' in existing[0]['checksums']):
        hashes['credit_default_model']=hashlib.sha256(json.dumps(model_metrics,sort_keys=True,ensure_ascii=False).encode()).hexdigest()
    for name,frame in personal.items():
        stable=frame.sort_values(PERSONAL_TABLES[name])
        hashes[name]=hashlib.sha256(stable.to_json(orient='records',date_format='iso').encode()).hexdigest()
    manifest = {'checks': checks, 'checksums': hashes,
                'source_paths': {name: lake_path('gold', dataset, run_id) for name, (dataset, _) in TABLES.items()}}
    if extended:
        manifest['personal_checks']=personal_checks
        manifest['lake_format']='Delta Lake 3.2.0'
        manifest['risk_model_version']=model_metrics['model_version']
        manifest['source_paths'].update({name:lake_path('gold',name,run_id) for name in PERSONAL_TABLES})
        manifest['source_paths'].update({name:lake_path('gold',name,run_id) for name in ['credit_default_features','credit_default_model']})
    with db_connect() as conn, conn.cursor() as cursor:
        # El puntero y todas las tablas cambian en la misma transacción.
        cursor.execute("SELECT pg_advisory_xact_lock(712031)")
        cursor.execute('SELECT manifest FROM gold.publications WHERE run_id=%s', (run_id,))
        previous = cursor.fetchone()
        if previous:
            if previous[0]['checksums'] != hashes:
                raise ValueError('Este run_id ya publicó otro contenido. Usa una corrida nueva.')
            print('Publicación idéntica ya existente. Reintento idempotente.')
            record_metrics(records_written=0, validation=checks)
            return
        cursor.execute('INSERT INTO gold.publications(run_id,manifest) VALUES (%s,%s)', (run_id, Json(manifest)))
        # Orden de columnas explícito, independiente del orden físico de Spark.
        names = ['period_month','transaction_count','income_transaction_count','expense_transaction_count',
                 'total_income','total_expenses','net_cash_flow','income_to_expense_ratio',
                 'estimated_net_cash_flow_rate_pct','financial_health_score']
        rows = [(run_id, *[None if pd.isna(row[c]) else row[c] for c in names])
                for row in frames['monthly_cashflow'].to_dict('records')]
        execute_values(cursor, 'INSERT INTO gold.monthly_cashflow(source_run_id,' + ','.join(names) + ') VALUES %s', rows, page_size=2000)
        names = ['period_month','category','expense_transaction_count','expense_total','expense_share_pct']
        rows = [(run_id, *[None if pd.isna(row[c]) else row[c] for c in names])
                for row in frames['category_spending'].to_dict('records')]
        if rows:
            execute_values(cursor, 'INSERT INTO gold.category_spending(source_run_id,' + ','.join(names) + ') VALUES %s', rows, page_size=2000)
        combined = frames['credit_cards'].merge(frames['anomalies'][['cust_id', 'is_anomaly', 'anomaly_score',
                                              'credit_utilization', 'payment_to_balance', 'minimum_payment_to_balance','isolation_score','decision_score','model_run_id','scored_at']],
                                              on='cust_id', validate='one_to_one')
        names = ['cust_id','balance','credit_limit','purchases','payments','credit_utilization_pct','cluster','is_anomaly','anomaly_score']
        rows = [(run_id, *[None if pd.isna(row[c]) else row[c] for c in names], Json(canonical_record(row)))
                for row in combined.to_dict('records')]
        execute_values(cursor, 'INSERT INTO gold.credit_cards(source_run_id,' + ','.join(names) + ',features) VALUES %s', rows, page_size=2000)
        anomaly_names=['cust_id','is_anomaly','anomaly_score','isolation_score','decision_score',
                       'credit_utilization','payment_to_balance','minimum_payment_to_balance','model_run_id','scored_at']
        anomaly_rows=[(run_id,*[None if pd.isna(row[c]) else row[c] for c in anomaly_names],Json(canonical_record(row)))
                      for row in frames['anomalies'].to_dict('records')]
        execute_values(cursor,'INSERT INTO gold.credit_card_anomalies(source_run_id,'+','.join(anomaly_names)+',features) VALUES %s',anomaly_rows,page_size=2000)
        names = ['user_id','record_date','monthly_income_usd','monthly_expenses_usd','savings_usd','credit_score','savings_capacity','emergency_fund_months']
        rows = [(run_id, *[row[c] for c in names], Json(canonical_record(row)))
                for row in frames['financial_profiles'].to_dict('records')]
        execute_values(cursor, 'INSERT INTO gold.financial_profiles(source_run_id,' + ','.join(names) + ',features) VALUES %s', rows, page_size=2000)
        if extended:
            insert_personal(cursor,run_id,personal,model_metrics)
        cursor.execute('INSERT INTO gold.active_publication(singleton,run_id) VALUES (true,%s) '
                       'ON CONFLICT(singleton) DO UPDATE SET run_id=excluded.run_id', (run_id,))
    record_metrics(records_written=sum(len(v) for v in frames.values()), validation=checks)
    print(json.dumps({'run_id': run_id, 'validation': checks}, ensure_ascii=False))
