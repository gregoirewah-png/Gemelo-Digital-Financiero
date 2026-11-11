"""Regresiones de finanzas personales: cifras, pagos y escenarios sin LLM."""
import copy
import json
from pathlib import Path
import pandas as pd
import pytest
import src.ml.personal_agent as service
from src.ml.monte_carlo import MonteCarloConfig, simulate


@pytest.fixture
def snapshot():
    profile=json.loads((Path(__file__).resolve().parents[1]/'data/demo/profile.json').read_text())
    profile.update(monthly_income=34000.,monthly_expenses=25000.,monthly_available=9000.,
                   first_month='2025-11-01',last_month='2026-10-01',credit_utilization_pct=24.8,
                   latest_month={'period_month':'2026-10-01','total_income':34000.,'total_expenses':25000.,'net_cash_flow':9000.},
                   credit_risk={'level':'Bajo','probability':.09})
    return {'publication':{'run_id':'fixture'},'profile':profile,'categories':[{'category':'Vivienda','expense_total':8500.}],
            'monthly':[{'period_month':'2026-09-01','total_income':34000.,'total_expenses':26000.,'net_cash_flow':8000.},
                       {'period_month':'2026-10-01','total_income':34000.,'total_expenses':25000.,'net_cash_flow':9000.}]}


@pytest.fixture
def agent(monkeypatch,snapshot):
    monkeypatch.setattr(service,'personal_snapshot',lambda *_:copy.deepcopy(snapshot))
    monkeypatch.delenv('GROQ_API_KEY',raising=False)
    return service.PersonalFinancialAgent('persona_demo')


@pytest.mark.parametrize('text,value',[('15,000',15000.),('15.000',15000.),('2 mil',2000.),('1.500,50',1500.5),('1,500.50',1500.5)])
def test_spanish_amounts(text,value):
    assert service.parse_money(text)==value


def test_loan_is_included_once(agent):
    response=agent.generar_respuesta('¿Cuánto puedo ahorrar?')
    assert '$9,000' in response and 'ya está incluido' in response
    assert '$6,500' not in response


def test_expenses_render_a_chart_with_evidence(agent):
    answer=agent.generar_respuesta('¿En qué estoy gastando más?')
    assert 'Vivienda' in answer and '$8,500' in answer
    assert agent.last_result['kind']=='categories'
    assert agent.last_evidence[0]['source_run_id']=='fixture'


def test_risk_question_about_card_keeps_risk_intent(agent):
    assert 'riesgo **bajo**' in agent.generar_respuesta('¿Cuál es el riesgo de mi tarjeta?')


def test_purchase_amount_followup(agent,monkeypatch):
    assert '¿Cuánto cuesta' in agent.generar_respuesta('¿Puedo comprar una laptop?')
    calls=[]
    def simulated(snapshot,params):
        calls.append(params)
        raise RuntimeError('Solo capturamos la configuración')
    monkeypatch.setattr(agent,'_simulate',simulated)
    agent.generar_respuesta('15,000')
    assert calls[0]['scenarios'][1]['one_time_expense']==15000


def test_recurring_payment_and_parameters(agent,snapshot):
    config=agent._parse_simulation(service.plain('¿Qué pasa si gasto 2,000 pesos más al mes durante 6 meses con semilla 17 y 1000 simulaciones?'),snapshot)
    assert config['months']==6 and config['seed']==17 and config['simulations']==1000
    assert config['scenarios'][1]['extra_monthly_expense']==2000
    assert config['scenarios'][1].get('one_time_expense',0)==0


def test_one_time_purchase_is_subtracted_once():
    history=pd.DataFrame({'period_month':pd.date_range('2026-01-01',periods=3,freq='MS'),
                          'total_income':[100.]*3,'total_expenses':[80.]*3})
    scenario={'name':'Compra','income_change_pct':0.,'expense_change_pct':0.,'extra_monthly_expense':0.,'one_time_expense':50.}
    result,_=simulate(history,MonteCarloConfig(initial_savings=100.,months=3,simulations=100,scenarios=[scenario]))
    assert result.p50.tolist()==[70.,90.,110.]


def test_open_question_offline_is_explicit(agent):
    assert 'no está disponible' in agent.generar_respuesta('Explícame cómo formar un hábito saludable')


def test_zero_starting_balance_is_respected(agent,snapshot):
    config=agent._parse_simulation('simula con ahorro inicial de 0 y 12 meses',snapshot)
    assert config['initial_savings']==0


def test_large_horizon_is_rejected_instead_of_silently_defaulting(agent,snapshot):
    config=agent._parse_simulation('simula 120 meses',snapshot)
    assert config['months']==120
    with pytest.raises(ValueError):
        MonteCarloConfig(**config).validate()
