"""Bootstrap mensual pareado de ingresos/gastos. No modifica la historia Gold."""
from dataclasses import asdict, dataclass, field
import hashlib
import json
import re

import numpy as np
import pandas as pd

MODEL_VERSION = 'paired-month-bootstrap-v2'
DEFAULT_SCENARIOS = [
    {'name': 'Base', 'income_change_pct': 0., 'expense_change_pct': 0., 'extra_monthly_expense': 0.},
    {'name': 'Gastos +10%', 'income_change_pct': 0., 'expense_change_pct': 10., 'extra_monthly_expense': 0.},
    {'name': 'Ingresos -10%', 'income_change_pct': -10., 'expense_change_pct': 0., 'extra_monthly_expense': 0.},
]


@dataclass
class MonteCarloConfig:
    initial_savings: float = 0.
    months: int = 12
    simulations: int = 5000
    seed: int = 42
    scenarios: list = field(default_factory=lambda: [dict(s) for s in DEFAULT_SCENARIOS])

    def validate(self):
        for name, low, high in [('months',1,60), ('simulations',100,50000), ('seed',0,2**32-1)]:
            value = getattr(self, name)
            if isinstance(value, bool) or not isinstance(value, int) or not low <= value <= high:
                raise ValueError(f'{name} debe ser entero entre {low} y {high}')
        if not np.isfinite(self.initial_savings) or not 0 <= self.initial_savings <= 1e12:
            raise ValueError('initial_savings debe ser finito, no negativo y menor o igual a 1e12')
        if not isinstance(self.scenarios, list) or not 1 <= len(self.scenarios) <= 5:
            raise ValueError('Configura entre 1 y 5 escenarios')
        names = set()
        for scenario in self.scenarios:
            required = {'name','income_change_pct','expense_change_pct','extra_monthly_expense'}
            if not required <= set(scenario) or set(scenario)-required-{'one_time_expense'}:
                raise ValueError('Escenario incompleto o con parámetros desconocidos')
            name = scenario['name']
            if not isinstance(name, str) or not name.strip() or len(name) > 80 or name in names:
                raise ValueError('Los nombres de escenarios deben ser únicos, de 1 a 80 caracteres')
            names.add(name)
            for field_name in ['income_change_pct','expense_change_pct']:
                value = scenario[field_name]
                if not np.isfinite(value) or not -100 <= value <= 200:
                    raise ValueError('Los cambios porcentuales deben estar entre -100 y 200')
            label = re.search(r'(gastos?|ingresos?)\s*([+-]\d+(?:[.,]\d+)?)\s*%', name, re.IGNORECASE)
            if label:
                parameter = 'expense_change_pct' if label[1].lower().startswith('gasto') else 'income_change_pct'
                labeled_value = float(label[2].replace(',','.'))
                if not np.isclose(scenario[parameter], labeled_value, rtol=0., atol=1e-9):
                    raise ValueError(f'El nombre {name} exige {parameter}={labeled_value}. Usa 10 para 10%, no 0.10.')
            extra = scenario['extra_monthly_expense']
            if not np.isfinite(extra) or not 0 <= extra <= 1e9:
                raise ValueError('El gasto adicional debe estar entre 0 y 1e9')
            one_time = scenario.get('one_time_expense', 0.)
            if not np.isfinite(one_time) or not 0 <= one_time <= 1e9:
                raise ValueError('La compra de una sola vez debe estar entre 0 y 1e9')


def simulate(history, configuration):
    configuration.validate()
    frame = history[['period_month','total_income','total_expenses']].copy()
    frame['period_month'] = pd.to_datetime(frame.period_month)
    frame = frame.sort_values('period_month').reset_index(drop=True)
    if len(frame) < 3 or len(frame) > 1200:
        raise ValueError('Se requieren de 3 a 1200 meses históricos')
    if frame.period_month.isna().any() or frame.period_month.duplicated().any():
        raise ValueError('Los periodos deben ser fechas válidas y únicas')
    if not (frame.period_month.dt.day == 1).all():
        raise ValueError('period_month debe ser el primer día de cada mes')
    expected = pd.date_range(frame.period_month.min(), frame.period_month.max(), freq='MS')
    if not frame.period_month.equals(pd.Series(expected, name='period_month')):
        raise ValueError('Hay meses sin datos. Confirma la cobertura antes de simular, sin imputar ceros.')
    pairs = frame[['total_income','total_expenses']].astype(float).to_numpy()
    if not np.isfinite(pairs).all() or (pairs < 0).any():
        raise ValueError('Ingresos y gastos históricos deben ser finitos y no negativos')
    canonical = frame.copy()
    canonical['total_income'] = pairs[:,0]
    canonical['total_expenses'] = pairs[:,1]
    history_hash = hashlib.sha256(canonical.to_json(orient='records', date_format='iso').encode()).hexdigest()
    rng = np.random.default_rng(configuration.seed)
    # Misma muestra aleatoria para todos los escenarios, facilitando comparaciones.
    indexes = rng.integers(len(pairs), size=(configuration.simulations, configuration.months))
    income = pairs[indexes,0]
    expenses = pairs[indexes,1]
    dates = pd.date_range(frame.period_month.max() + pd.offsets.MonthBegin(1), periods=configuration.months, freq='MS')
    records = []
    for scenario in configuration.scenarios:
        net = (income * (1 + scenario['income_change_pct']/100)
               - expenses * (1 + scenario['expense_change_pct']/100) - scenario['extra_monthly_expense'])
        net[:,0] -= scenario.get('one_time_expense',0.)
        balances = configuration.initial_savings + np.cumsum(net, axis=1)
        percentiles = np.quantile(balances, [.05,.5,.95], axis=0)
        negative = balances < 0
        ever_negative = np.maximum.accumulate(negative, axis=1)
        for index, date in enumerate(dates):
            records.append({'scenario':scenario['name'], 'month':index+1, 'period_month':date.date(),
                            'p05':float(percentiles[0,index]), 'p50':float(percentiles[1,index]),
                            'p95':float(percentiles[2,index]), 'mean_balance':float(balances[:,index].mean()),
                            'probability_negative':float(negative[:,index].mean()),
                            'probability_ever_negative':float(ever_negative[:,index].mean())})
    metadata = {'configuration':asdict(configuration), 'historical_months':len(frame),
                'history_hash':history_hash, 'model_version':MODEL_VERSION,
                'first_month':str(frame.period_month.min().date()), 'last_month':str(frame.period_month.max().date())}
    return pd.DataFrame(records), metadata
