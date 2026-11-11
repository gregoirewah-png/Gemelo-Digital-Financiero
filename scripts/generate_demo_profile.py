"""Genera una persona ficticia reproducible y sus fuentes financieras coherentes."""
import calendar
import csv
from datetime import date
import json
from pathlib import Path
import random

from dotenv import dotenv_values

ROOT = Path(__file__).resolve().parents[1]


def generate(root=ROOT):
    settings = dotenv_values(root/'.env', interpolate=False)
    profile_id = settings.get('DEMO_PROFILE_ID', 'persona_demo')
    name = settings.get('DEMO_PROFILE_NAME', 'Alex')
    currency = settings.get('DEMO_CURRENCY', 'MXN')
    end = date.fromisoformat(settings.get('DEMO_END_MONTH', '2026-10-01'))
    months = int(settings.get('DEMO_MONTHS', '12'))
    seed = int(settings.get('DEMO_SEED', '42'))
    rng = random.Random(seed)
    folder = root/'data/demo'
    folder.mkdir(parents=True, exist_ok=True)
    rows, cards = [], []
    opening_cash = 5000.0
    cash = opening_cash
    loan = 60000.0
    card_balance, card_limit = 6200.0, 25000.0
    categories = [('Vivienda', 8500., 'Transferencia'), ('Supermercado', 4300., 'Tarjeta'),
                  ('Transporte', 2300., 'Débito'), ('Servicios', 2100., 'Débito'),
                  ('Comida fuera', 1700., 'Tarjeta'), ('Entretenimiento', 1300., 'Tarjeta'),
                  ('Salud', 900., 'Débito'), ('Compras', 1400., 'Tarjeta')]
    for offset in range(months-1, -1, -1):
        ordinal = end.year*12 + end.month-1-offset
        year, month_index = divmod(ordinal, 12)
        month = month_index+1
        income = round(34000 + rng.uniform(-600,600), 2)
        expenses = []
        for category, base, method in categories:
            amount = round(base * rng.uniform(.90,1.10), 2)
            expenses.append((category, amount, method))
        interest = round(card_balance*.28/12, 2)
        expenses += [('Pago de préstamo',2500.,'Transferencia'), ('Intereses de tarjeta',interest,'Débito')]
        transactions = [('Ingreso','Sueldo',income,'Transferencia')] + [('Gasto',*item) for item in expenses]
        for index, (kind, category, amount, method) in enumerate(transactions):
            day = 1 if kind=='Ingreso' else min(3+index*2,27)
            rows.append({'profile_id':profile_id,'transaction_id':f'{profile_id}-{year}{month:02d}-{index:02d}',
                         'date':date(year,month,day).isoformat(),'type':kind,'category':category,
                         'amount':amount,'payment_method':method})
        total_expenses = round(sum(item[1] for item in expenses),2)
        cash = round(cash + income-total_expenses,2)
        loan = round(max(0.,loan*(1+.18/12)-2500),2)
        purchases = round(sum(amount for category,amount,method in expenses if method=='Tarjeta'),2)
        cards.append({'profile_id':profile_id,'period_month':date(year,month,1).isoformat(),
                      'credit_limit':card_limit,'balance':card_balance,'purchases':purchases,
                      'payments':round(purchases+interest,2),'minimum_payment':round(card_balance*.05,2),
                      'delay_months':0})
    end_date = date(end.year,end.month,calendar.monthrange(end.year,end.month)[1])
    profile = {'profile_id':profile_id,'display_name':name,'currency':currency,'as_of_date':str(end_date),
               'data_kind':'synthetic_demo','seed':seed,'opening_cash':opening_cash,
               'available_savings':cash,'loan_balance':loan,'monthly_loan_payment':2500.,
               'card_balance':card_balance,'card_limit':card_limit,'card_payment_day':28,
               'net_worth':round(cash-loan-card_balance,2),
               'description':'Persona y operaciones ficticias creadas para la demostración académica.',
               'expense_definition':'Los gastos incluyen la cuota del préstamo y los intereses. Se descuentan una sola vez.'}
    for name, records in [('transactions.csv',rows),('cards.csv',cards)]:
        with (folder/name).open('w',newline='',encoding='utf-8') as handle:
            writer=csv.DictWriter(handle,fieldnames=list(records[0]))
            writer.writeheader()
            writer.writerows(records)
    (folder/'profile.json').write_text(json.dumps(profile,ensure_ascii=False,indent=2)+'\n')
    print('Perfil ficticio generado',profile_id,'movimientos',len(rows),'meses',months)
    return profile


if __name__=='__main__':
    generate()
