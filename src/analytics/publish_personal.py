"""Validación y publicación SQL de la persona y el modelo supervisado."""
import json
import os
import numpy as np

from psycopg2.extras import Json, execute_values

from src.transformation.personal_profile import PERSONAL_TABLES


def validate_personal(frames, model_metrics):
    for name,key in PERSONAL_TABLES.items():
        frame=frames[name]
        if frame.empty or frame[key].isna().any().any() or frame.duplicated(key).any():
            raise ValueError(f'Grano o llave inválidos en {name}')
    profiles={row.profile_id:json.loads(row.payload_json) for row in frames['personal_profile'].itertuples()}
    monthly=frames['personal_monthly_cashflow']
    categories=frames['personal_category_spending']
    cards=frames['personal_card_history']
    if not set(monthly.profile_id)==set(cards.profile_id)==set(categories.profile_id)==set(profiles):
        raise ValueError('Las fuentes personales no comparten las mismas identidades')
    if set(zip(monthly.profile_id,monthly.period_month))!=set(zip(cards.profile_id,cards.period_month)):
        raise ValueError('La tarjeta y el flujo no cubren los mismos meses')
    numeric=['credit_limit','balance','purchases','payments','minimum_payment','delay_months']
    values=cards[numeric].astype(float)
    if not np.isfinite(values.to_numpy()).all() or (values<0).any().any() or (values.credit_limit<=0).any():
        raise ValueError('Variables personales de tarjeta fuera de contrato')
    sums=categories.groupby(['profile_id','period_month']).expense_total.sum()
    for row in monthly.itertuples():
        if abs(float(row.total_income)-float(row.total_expenses)-float(row.net_cash_flow))>.011:
            raise ValueError('Flujo personal sin conciliación')
        if abs(float(sums.get((row.profile_id,row.period_month),0))-float(row.total_expenses))>.011:
            raise ValueError('Categorías personales sin conciliación')
    for profile_id,payload in profiles.items():
        selected=monthly[monthly.profile_id==profile_id]
        closing=payload['opening_cash']+float(selected.net_cash_flow.sum())
        if abs(closing-payload['available_savings'])>.011 or payload['data_kind']!='synthetic_demo':
            raise ValueError('El saldo o la procedencia de la persona no concilian')
        if not 0<=payload['credit_risk']['probability']<=1:
            raise ValueError('Probabilidad de riesgo fuera de rango')
    if model_metrics['roc_auc']<float(os.getenv('RISK_MIN_AUC','.65')) or model_metrics['brier_score']>=model_metrics['baseline_brier']:
        raise ValueError('Modelo supervisado sin calidad mínima')
    return {'identity':'PASSED','cash_reconciliation':'PASSED','category_reconciliation':'PASSED',
            'source_kind':'synthetic_demo','profiles':len(profiles),'risk_model':model_metrics['model_version']}


def insert_personal(cursor,run_id,frames,model_metrics):
    profiles=[(run_id,row.profile_id,Json(json.loads(row.payload_json)))
              for row in frames['personal_profile'].itertuples()]
    execute_values(cursor,'INSERT INTO gold.personal_profiles(source_run_id,profile_id,payload) VALUES %s',profiles)
    columns={
        'personal_monthly_cashflow':['profile_id','period_month','total_income','total_expenses',
                                     'loan_payment_total','transaction_count','net_cash_flow'],
        'personal_category_spending':['profile_id','period_month','category','expense_total'],
        'personal_card_history':['profile_id','period_month','credit_limit','balance','purchases',
                                 'payments','minimum_payment','delay_months'],
    }
    for table,names in columns.items():
        rows=[(run_id,*[row[name] for name in names]) for row in frames[table].to_dict('records')]
        execute_values(cursor,'INSERT INTO gold.'+table+'(source_run_id,'+','.join(names)+') VALUES %s',rows)
    cursor.execute('INSERT INTO gold.credit_default_models(source_run_id,metrics) VALUES (%s,%s)',
                   (run_id,Json(model_metrics)))
