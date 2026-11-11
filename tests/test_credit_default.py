"""Contrato del modelo supervisado y separación de variables financieras."""
import numpy as np
import pandas as pd
import pytest
from src.ml.credit_default_model import build_credit_features


def row():
    return {'X1':25000,**{f'X{i}':0 for i in range(6,12)},
            **{f'X{i}':6200 for i in range(12,18)},**{f'X{i}':9000 for i in range(18,24)}}


def test_financial_features_ignore_demographics_and_target():
    raw=pd.DataFrame([{**row(),'X2':1,'X3':2,'X4':1,'X5':30,'Y':1}])
    a=build_credit_features(raw)
    raw[['X2','X3','X4','X5','Y']]=999
    pd.testing.assert_frame_equal(a,build_credit_features(raw))
    assert len(a.columns)==20
    assert a.utilization_1.iloc[0]==pytest.approx(.248)
    assert a.late_months.iloc[0]==0


@pytest.mark.parametrize('key,value',[('X1',0),('X1',np.inf),('X18',-1),('X6',10),('X12',np.nan)])
def test_invalid_financial_variables_are_rejected(key,value):
    source=row();source[key]=value
    with pytest.raises(ValueError):
        build_credit_features(pd.DataFrame([source]))


def test_zero_statement_balance_has_finite_payment_ratio():
    source=row();source['X12']=0
    result=build_credit_features(pd.DataFrame([source]))
    assert result.payment_share_1.iloc[0]==0
    assert np.isfinite(result.to_numpy()).all()
