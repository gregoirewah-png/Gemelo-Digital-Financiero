"""Gold personal coherente. Concilia movimientos, ahorros, préstamo y tarjeta."""
import json

import joblib
from minio import Minio
import pandas as pd
from pyspark.sql import functions as F

from src.common.config import ROOT, lake_path, required, safe_run_id
from src.common.observability import record_metrics
from src.common.spark import spark_session, write_lake
from src.ml.credit_default_model import score_profile

PERSONAL_TABLES={
    'personal_profile':['profile_id'],
    'personal_monthly_cashflow':['profile_id','period_month'],
    'personal_category_spending':['profile_id','period_month','category'],
    'personal_card_history':['profile_id','period_month'],
}


def build_personal(run_id):
    spark=spark_session('Gold_Personal_360')
    try:
        endpoint=required('MINIO_ENDPOINT')
        client=Minio(endpoint.split('://',1)[-1],access_key=required('MINIO_ROOT_USER'),
                     secret_key=required('MINIO_ROOT_PASSWORD'),secure=endpoint.startswith('https://'))
        key=lake_path('bronze','PersonalProfile',run_id).removeprefix('s3a://bronze/')+'/profile.json'
        response=client.get_object('bronze',key)
        try:
            profile=json.loads(response.read())
        finally:
            response.close()
            response.release_conn()
        raw=(spark.read.option('header',True).csv(lake_path('bronze','PersonalTransactions',run_id)+'/source.csv')
             .withColumn('date',F.to_date('date')).withColumn('amount',F.col('amount').cast('decimal(18,2)')))
        count=raw.count()
        bad=raw.filter(F.col('date').isNull() | F.col('amount').isNull() | (F.col('amount')<0)
                       | (F.col('profile_id')!=profile['profile_id']) | ~F.col('type').isin('Ingreso','Gasto')).count()
        if bad or raw.select('transaction_id').distinct().count()!=count:
            raise ValueError('Las fuentes de la persona no cumplen su contrato de identidad y movimientos')
        raw=raw.withColumn('period_month',F.trunc('date','month'))
        monthly=raw.groupBy('profile_id','period_month').agg(
            F.sum(F.when(F.col('type')=='Ingreso',F.col('amount')).otherwise(0)).cast('decimal(18,2)').alias('total_income'),
            F.sum(F.when(F.col('type')=='Gasto',F.col('amount')).otherwise(0)).cast('decimal(18,2)').alias('total_expenses'),
            F.sum(F.when(F.col('category')=='Pago de préstamo',F.col('amount')).otherwise(0)).cast('decimal(18,2)').alias('loan_payment_total'),
            F.count('*').alias('transaction_count'))
        monthly=monthly.withColumn('net_cash_flow',(F.col('total_income')-F.col('total_expenses')).cast('decimal(18,2)'))
        categories=raw.filter(F.col('type')=='Gasto').groupBy('profile_id','period_month','category').agg(
            F.sum('amount').cast('decimal(18,2)').alias('expense_total'))
        cards=(spark.read.option('header',True).csv(lake_path('bronze','PersonalCards',run_id)+'/source.csv')
               .withColumn('period_month',F.to_date('period_month')))
        for name in ['credit_limit','balance','purchases','payments','minimum_payment']:
            cards=cards.withColumn(name,F.col(name).cast('decimal(18,2)'))
        cards=cards.withColumn('delay_months',F.col('delay_months').cast('int'))
        card_data=cards.toPandas()
        month_data=monthly.orderBy('period_month').toPandas()
        if not (card_data.profile_id==profile['profile_id']).all() or card_data.period_month.duplicated().any():
            raise ValueError('La tarjeta no corresponde a la misma persona y periodo')
        closing=round(profile['opening_cash']+float(month_data.net_cash_flow.sum()),2)
        if abs(closing-profile['available_savings'])>.01:
            raise ValueError('No concilian el saldo inicial, los movimientos y el saldo final')
        month_data=month_data.sort_values('period_month')
        latest=month_data.iloc[-1]
        profile['monthly_income']=round(float(month_data.total_income.mean()),2)
        profile['monthly_expenses']=round(float(month_data.total_expenses.mean()),2)
        profile['monthly_available']=round(float(month_data.net_cash_flow.mean()),2)
        profile['latest_month']={key:(str(value) if key=='period_month' else float(value))
                                 for key,value in latest.items() if key!='profile_id'}
        profile['historical_months']=len(month_data)
        profile['first_month']=str(month_data.iloc[0].period_month)
        profile['last_month']=str(latest.period_month)
        profile['credit_utilization_pct']=round(100*profile['card_balance']/profile['card_limit'],2)
        bundle=joblib.load(ROOT/'models/credit_default'/safe_run_id(run_id)/'model.joblib')
        profile['credit_risk']=score_profile(bundle,card_data)
        profile['source_run_id']=run_id
        identity=spark.createDataFrame([(profile['profile_id'],json.dumps(profile,ensure_ascii=False))],
                                       ['profile_id','payload_json'])
        frames={'personal_profile':identity,'personal_monthly_cashflow':monthly,
                'personal_category_spending':categories,'personal_card_history':cards}
        for name,frame in frames.items():
            write_lake(frame,lake_path('gold',name,run_id))
        record_metrics(records_read=count,records_written=sum(frame.count() for frame in frames.values()),
                       profile_id=profile['profile_id'],identity_checks='PASSED',cash_reconciliation='PASSED')
        print('Perfil 360 conciliado',profile['profile_id'],'meses',len(month_data),'saldo',closing)
    finally:
        spark.stop()
