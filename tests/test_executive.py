from src.analytics.executive_metrics import business_findings


def test_business_alerts_distinguish_portfolios_and_do_not_claim_fraud():
    snapshot={'monthly':[{'total_income':100,'total_expenses':80,'net_cash_flow':20},
                         {'total_income':100,'total_expenses':120,'net_cash_flow':-20}],
              'segments':[{'customers':100,'anomalies':2}], 'profiles':{'negative_capacity':3}}
    result=business_findings(snapshot)
    assert result['net']==-20 and result['expense_change_pct']==50
    assert result['anomaly_rate']==2
    assert len(result['alerts'])==3
    assert 'no demuestra fraude' in result['alerts'][1]['meaning']
    assert 'USD' in result['alerts'][2]['meaning']
