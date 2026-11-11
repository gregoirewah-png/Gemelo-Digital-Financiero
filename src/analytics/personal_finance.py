"""Lecturas de solo consulta para el perfil personal y escenarios publicados."""
from psycopg2.extras import RealDictCursor

from src.common.config import db_connect, required


def personal_snapshot(profile_id=None, source_run_id=None):
    profile_id=profile_id or required('DEMO_PROFILE_ID')
    with db_connect(reader=True) as conn, conn.cursor(cursor_factory=RealDictCursor) as cursor:
        cursor.execute('SET TRANSACTION ISOLATION LEVEL REPEATABLE READ')
        if source_run_id is None:
            cursor.execute('SELECT run_id,published_at FROM serving.publication')
        else:
            cursor.execute('SELECT run_id,published_at FROM serving.history_publications WHERE run_id=%s',(source_run_id,))
        publication=cursor.fetchone()
        if not publication:
            raise ValueError('El perfil todavía se está preparando')
        run_id=publication['run_id']
        cursor.execute('SELECT payload FROM serving.history_personal_profiles WHERE source_run_id=%s AND profile_id=%s',
                       (run_id,profile_id))
        selected=cursor.fetchone()
        if not selected:
            raise ValueError('El perfil personal todavía se está preparando')
        cursor.execute('SELECT period_month,total_income,total_expenses,loan_payment_total,net_cash_flow,transaction_count '
                       'FROM serving.history_personal_monthly_cashflow WHERE source_run_id=%s AND profile_id=%s '
                       'ORDER BY period_month',(run_id,profile_id))
        monthly=[dict(row) for row in cursor.fetchall()]
        cursor.execute('SELECT category,SUM(expense_total) AS expense_total '
                       'FROM serving.history_personal_category_spending WHERE source_run_id=%s AND profile_id=%s '
                       'AND period_month=%s GROUP BY category ORDER BY expense_total DESC',
                       (run_id,profile_id,monthly[-1]['period_month']))
        categories=[dict(row) for row in cursor.fetchall()]
        cursor.execute('SELECT period_month,credit_limit,balance,purchases,payments,minimum_payment,delay_months '
                       'FROM serving.history_personal_card_history WHERE source_run_id=%s AND profile_id=%s ORDER BY period_month',
                       (run_id,profile_id))
        cards=[dict(row) for row in cursor.fetchall()]
    return {'publication':dict(publication),'profile':selected['payload'],'monthly':monthly,
            'categories':categories,'cards':cards}


def saved_simulations(profile_id=None, simulation_run_id=None, limit=10):
    profile_id=profile_id or required('DEMO_PROFILE_ID')
    query='SELECT * FROM simulation.runs WHERE profile_id=%s'
    params=[profile_id]
    if simulation_run_id:
        query+=' AND simulation_run_id=%s'
        params.append(simulation_run_id)
    query+=' ORDER BY created_at DESC LIMIT %s'
    params.append(min(max(int(limit),1),50))
    with db_connect(reader=True) as conn, conn.cursor(cursor_factory=RealDictCursor) as cursor:
        cursor.execute(query,params)
        runs=[dict(row) for row in cursor.fetchall()]
        for run in runs:
            cursor.execute('SELECT scenario,month,period_month,p05,p50,p95,mean_balance,probability_negative,'
                           'probability_ever_negative FROM simulation.results WHERE simulation_run_id=%s ORDER BY scenario,month',
                           (run['simulation_run_id'],))
            run['results']=[dict(row) for row in cursor.fetchall()]
    return runs
