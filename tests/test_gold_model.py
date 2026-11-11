import pandas as pd
import pytest
from src.analytics.publish_gold import validate_model


def frames():
    return {
        'monthly_cashflow':pd.DataFrame([{'period_month':'2024-01-01','total_income':100,'total_expenses':80,'net_cash_flow':20}]),
        'category_spending':pd.DataFrame([{'period_month':'2024-01-01','category':'Food','expense_total':80}]),
        'credit_cards':pd.DataFrame([{'cust_id':'C1'}]),
        'anomalies':pd.DataFrame([{'cust_id':'C1','credit_utilization':.2,'payment_to_balance':.4,
            'minimum_payment_to_balance':.1,'is_anomaly':False,'anomaly_score':.2}]),
        'financial_profiles':pd.DataFrame([{'user_id':'U1','record_date':'2024-01-01',
            'savings_capacity':100,'emergency_fund_months':2,'credit_score':700}]),
    }


def test_dimensional_relationships():
    result=validate_model(frames())
    assert result['relationships']['card_anomaly_1_to_1']=='valid'
    assert result['financial_profiles']['grain']==['user_id','record_date']


@pytest.mark.parametrize('kind',['duplicate','orphan','cashflow','card_feature'])
def test_invalid_model_blocks_publication(kind):
    data=frames()
    if kind=='duplicate':
        data['financial_profiles']=pd.concat([data['financial_profiles']]*2)
    elif kind=='orphan':
        data['category_spending']['period_month']='2024-02-01'
    elif kind=='cashflow':
        data['monthly_cashflow']['net_cash_flow']=999
    else:
        data['anomalies']['cust_id']='C2'
    with pytest.raises(ValueError):
        validate_model(data)
