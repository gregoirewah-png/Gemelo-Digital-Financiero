"""Aceptación del perfil publicado. Sin llamadas externas al proveedor de IA."""
import hashlib
import json
from dataclasses import asdict

from minio import Minio
import pandas as pd

from src.analytics.personal_finance import personal_snapshot, saved_simulations
from src.analytics.run_monte_carlo import run_simulation
from src.common.config import db_connect, required, lake_path
from src.ml.agent_service import query_gold
from src.ml.monte_carlo import MonteCarloConfig
from src.ml.personal_agent import PersonalFinancialAgent, money


def check():
    snapshot=personal_snapshot()
    profile=snapshot['profile']
    run_id=snapshot['publication']['run_id']
    assert profile['data_kind']=='synthetic_demo'
    assert len(snapshot['monthly'])==len(snapshot['cards'])==12
    closing=profile['opening_cash']+sum(float(row['net_cash_flow']) for row in snapshot['monthly'])
    assert abs(closing-profile['available_savings'])<.011
    assert abs(sum(float(row['expense_total']) for row in snapshot['categories'])-profile['latest_month']['total_expenses'])<.011
    with db_connect(reader=True) as conn,conn.cursor() as cursor:
        cursor.execute('SELECT metrics FROM serving.credit_default_models')
        metrics=cursor.fetchone()[0]
        assert metrics['rows']==30000 and metrics['roc_auc']>=.65
        assert metrics['brier_score']<metrics['baseline_brier']
    for dataset,view in [('dataset1','monthly'),('dataset1','category_monthly'),('dataset2','anomalies'),('dataset3','summary'),('dataset4','summary'),('personal','monthly'),('personal','cards')]:
        evidence=query_gold(dataset,view,source_run_id=run_id)
        assert evidence['rows'] and evidence['publication']['run_id']==run_id
    def digest():
        current=personal_snapshot(profile['profile_id'],run_id)
        return hashlib.sha256(json.dumps(current,sort_keys=True,default=str).encode()).hexdigest()
    before=digest()
    config=MonteCarloConfig(initial_savings=profile['available_savings'],months=6,simulations=500,seed=17,
        scenarios=[{'name':'Actual','income_change_pct':0.,'expense_change_pct':0.,'extra_monthly_expense':0.},
                   {'name':'Compra','income_change_pct':0.,'expense_change_pct':0.,'extra_monthly_expense':0.,'one_time_expense':15000.}])
    result=run_simulation(source_run_id=run_id,profile_id=profile['profile_id'],configuration=config)
    rows=pd.DataFrame(result['results'])
    base=rows[rows.scenario=='Actual'].sort_values('month').p50.to_numpy()
    adjusted=rows[rows.scenario=='Compra'].sort_values('month').p50.to_numpy()
    assert abs((base-adjusted)-15000).max()<.011
    retry=run_simulation(source_run_id=run_id,profile_id=profile['profile_id'],configuration=config,
                         simulation_run_id=result['simulation_run_id'])
    assert result['results']==retry['results']
    saved=saved_simulations(profile['profile_id'],result['simulation_run_id'])
    assert len(saved)==1 and saved[0]['profile_id']==profile['profile_id']
    assert saved_simulations('otra_persona',result['simulation_run_id'])==[]
    assert digest()==before
    agent=PersonalFinancialAgent(profile['profile_id'])
    agent.client=None
    questions={'¿Cómo voy con mis finanzas?':money(profile['latest_month']['total_income']),
               '¿Cuánto puedo ahorrar al mes?':money(profile['monthly_available']),
               '¿En qué estoy gastando más?':snapshot['categories'][0]['category'],
               '¿Cuánto debo en mi tarjeta?':money(profile['card_balance']),
               'Muéstrame mi última simulación':'escenario guardado',
               '¿Qué riesgo tiene mi tarjeta?':'riesgo',
               'Quiero juntar 150,000 pesos':'150,000'}
    for question,expected in questions.items():
        answer=agent.generar_respuesta(question)
        assert expected.lower() in answer.lower(),(question,answer)
        assert not any(term in answer.lower() for term in ['gold','sql','dataset','traceback'])
    endpoint=required('MINIO_ENDPOINT')
    client=Minio(endpoint.split('://',1)[-1],access_key=required('MINIO_ROOT_USER'),
                 secret_key=required('MINIO_ROOT_PASSWORD'),secure=endpoint.startswith('https://'))
    tables=['personal_transactions','personal_transactions_category_spending','credit_card_balances',
            'credit_card_anomalies','financial_profile','personal_profile','personal_monthly_cashflow',
            'personal_category_spending','personal_card_history','credit_default_features','credit_default_model']
    for table in tables:
        prefix=lake_path('gold',table,run_id).removeprefix('s3a://gold/')
        client.stat_object('gold',prefix+'/_delta_log/00000000000000000000.json')
    report={'publication':run_id,'profile_id':profile['profile_id'],'monthly_rows':len(snapshot['monthly']),
            'available_savings':profile['available_savings'],'monthly_available':profile['monthly_available'],
            'roc_auc':metrics['roc_auc'],'brier_score':metrics['brier_score'],'baseline_brier':metrics['baseline_brier'],
            'test_rows':metrics['test_rows'],'delta_gold_tables':len(tables),'local_questions_passed':len(questions),
            'personal_hash_unchanged':before,'simulation_run_id':result['simulation_run_id'],
            'checks':['cash_reconciliation','category_reconciliation','all_gold_queries','model_quality',
                      'one_time_expense_once','simulation_idempotency','profile_isolation','gold_immutable','delta_logs']}
    print(json.dumps(report,ensure_ascii=False,indent=2))


if __name__=='__main__':
    check()
