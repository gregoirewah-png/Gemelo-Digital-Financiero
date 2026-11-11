"""Ejecutar: python tests/test_features.py (desde el paquete)."""
import sys
from pathlib import Path
sys.path.insert(0, str(Path(__file__).resolve().parents[1] / 'src' / 'ml'))
import numpy as np
import pandas as pd
from credit_card_anomaly_features import build_features, fit_score

rng = np.random.default_rng(17)
n = 500
example = pd.DataFrame({
    'cust_id':[f'C{i:05d}' for i in range(n)],
    'balance': rng.lognormal(5, 1, n),
    'credit_limit': rng.lognormal(8, .3, n),
    'purchases': rng.lognormal(5, .5, n),
    'cash_advance': rng.exponential(100, n),
    'payments': rng.lognormal(4.5, .5, n),
    'minimum_payments': rng.exponential(20, n),
    'purchases_trx': rng.poisson(10, n),
    'prc_full_payment': rng.beta(2, 4, n),
})
example.loc[0, 'balance'] = 0
example.loc[1, 'minimum_payments'] = np.nan
example.loc[2, 'balance'] = 90000
prepared, features, diagnosis = build_features(example)
assert np.isnan(prepared.loc[0, 'payment_to_balance'])
assert not np.isinf(features.select_dtypes(include='number')).any().any()
scored, metrics, bundle = fit_score(example, contamination=.02, stability_test=True)
assert len(scored) == n
assert 0 < metrics['n_anomalies'] < n
assert metrics['n_estimators'] == 200
assert scored['anomaly_score'].notna().all()
print('OK: prueba con datos ficticios')
print('Perfiles:', metrics['n_rows'])
print('Anomalías:', metrics['n_anomalies'])
print('Estabilidad top-k:', metrics['top_k_stability_jaccard_mean'])
