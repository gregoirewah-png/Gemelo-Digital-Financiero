import numpy as np
import pandas as pd
import pytest
from src.ml.monte_carlo import MonteCarloConfig, simulate


def history():
    return pd.DataFrame({'period_month':pd.date_range('2024-01-01',periods=6,freq='MS'),
                         'total_income':[100,150,100,200,120,160],
                         'total_expenses':[80,140,120,100,110,130]})


def test_reproducibility_and_immutable_source():
    data=history()
    original=data.copy(deep=True)
    a,meta=simulate(data,MonteCarloConfig(simulations=500,seed=17))
    b,other=simulate(data,MonteCarloConfig(simulations=500,seed=17))
    pd.testing.assert_frame_equal(a,b)
    pd.testing.assert_frame_equal(data,original)
    assert meta==other
    assert (a.p05<=a.p50).all() and (a.p50<=a.p95).all()
    assert a.probability_negative.between(0,1).all()


def test_constant_cashflow_and_initial_balance():
    data=history().assign(total_income=100,total_expenses=80)
    config=MonteCarloConfig(initial_savings=500,months=3,simulations=100,
        scenarios=[{'name':'Base','income_change_pct':0.,'expense_change_pct':0.,'extra_monthly_expense':0.}])
    result,_=simulate(data,config)
    assert result.p50.tolist()==[520.,540.,560.]
    assert result.p05.tolist()==result.p95.tolist()


def test_stress_scenario_paired_draws():
    result,_=simulate(history(),MonteCarloConfig(simulations=500))
    base=result[result.scenario=='Base'].reset_index(drop=True)
    stressed=result[result.scenario=='Gastos +10%'].reset_index(drop=True)
    assert (stressed.p50<=base.p50).all()
    assert (stressed.probability_ever_negative>=base.probability_ever_negative).all()
    assert (stressed.probability_ever_negative.diff().dropna()>=0).all()


@pytest.mark.parametrize('modification',[
    lambda h:h.drop(index=2),lambda h:h.assign(total_income=np.inf),
    lambda h:h.assign(total_expenses=-1),lambda h:pd.concat([h,h.iloc[:1]]),
])
def test_invalid_history(modification):
    with pytest.raises(ValueError):
        simulate(modification(history()),MonteCarloConfig())


@pytest.mark.parametrize('kwargs',[{'seed':-1},{'months':0},{'simulations':1},
    {'initial_savings':float('nan')},{'months':1.5},{'simulations':True},{'scenarios':[]}])
def test_invalid_configuration(kwargs):
    with pytest.raises(ValueError):
        simulate(history(),MonteCarloConfig(**kwargs))


def test_percentage_label_must_match_percentage_points():
    config=MonteCarloConfig(scenarios=[{'name':'Gastos +10%', 'income_change_pct':0.,
                                      'expense_change_pct':.1, 'extra_monthly_expense':0.}])
    with pytest.raises(ValueError,match='Usa 10 para 10%'):
        simulate(history(),config)
