"""Una fotografía coherente para explicar tendencias y señales de negocio."""
from psycopg2.extras import RealDictCursor
from src.common.config import db_connect


def executive_snapshot():
    with db_connect(reader=True) as conn,conn.cursor(cursor_factory=RealDictCursor) as q:
        q.execute('SET TRANSACTION ISOLATION LEVEL REPEATABLE READ')
        q.execute('SELECT run_id,published_at FROM serving.publication')
        publication=q.fetchone()
        if not publication:raise ValueError('Aún no hay una publicación analítica disponible')
        run=publication['run_id']
        def rows(sql):
            q.execute(sql,(run,));return [dict(row) for row in q.fetchall()]
        monthly=rows('SELECT period_month,total_income,total_expenses,net_cash_flow FROM serving.history_monthly_cashflow WHERE source_run_id=%s ORDER BY period_month')
        categories=rows('SELECT category,SUM(expense_total) AS expense_total FROM serving.history_category_spending WHERE source_run_id=%s GROUP BY category ORDER BY expense_total DESC')
        segments=rows('SELECT cluster,COUNT(*) AS customers,AVG(credit_utilization_pct) AS utilization,COUNT(*) FILTER(WHERE is_anomaly) AS anomalies FROM serving.history_credit_cards WHERE source_run_id=%s GROUP BY cluster ORDER BY cluster')
        profiles=rows('SELECT COUNT(*) AS profiles,COUNT(*) FILTER(WHERE savings_capacity<0) AS negative_capacity,AVG(savings_capacity) AS average_capacity FROM serving.history_financial_profiles WHERE source_run_id=%s')[0]
        models=rows('SELECT metrics FROM serving.history_credit_default_models WHERE source_run_id=%s')
        person=rows('SELECT payload FROM serving.history_personal_profiles WHERE source_run_id=%s ORDER BY profile_id LIMIT 1')
        q.execute('SELECT simulation_run_id,configuration,created_at FROM simulation.runs WHERE source_run_id=%s AND profile_id IS NOT NULL ORDER BY created_at DESC LIMIT 1',(run,))
        simulation=q.fetchone()
        results=[]
        if simulation:
            q.execute('SELECT scenario,month,period_month,p05,p50,p95,probability_ever_negative FROM simulation.results WHERE simulation_run_id=%s ORDER BY scenario,month',(simulation['simulation_run_id'],))
            results=[dict(row) for row in q.fetchall()]
    return {'publication':dict(publication),'monthly':monthly,'categories':categories,'segments':segments,
            'profiles':profiles,'model':models[0]['metrics'] if models else None,'person':person[0]['payload'] if person else None,
            'simulation':dict(simulation) if simulation else None,'results':results}


def business_findings(snapshot):
    current=snapshot['monthly'][-1]
    income=float(current['total_income']);expense=float(current['total_expenses']);net=float(current['net_cash_flow'])
    previous=snapshot['monthly'][-2] if len(snapshot['monthly'])>1 else None
    growth=None
    if previous and float(previous['total_expenses'])>0:
        growth=100*(expense/float(previous['total_expenses'])-1)
    total=sum(row['customers'] for row in snapshot['segments'])
    anomalies=sum(row['anomalies'] for row in snapshot['segments'])
    alerts=[]
    if net<0:
        alerts.append({'level':'Atención','title':'El gasto supera al ingreso del último mes','meaning':'El periodo presenta un faltante. Conviene revisar los rubros de gasto y la cobertura del historial antes de decidir acciones.'})
    else:
        alerts.append({'level':'Seguimiento','title':'El último mes conserva margen positivo','meaning':'El ingreso cubre el gasto observado. Ese flujo no equivale por sí solo al saldo de una cuenta.'})
    if anomalies:
        alerts.append({'level':'Revisión','title':f'{anomalies:,} perfiles de tarjetas atípicos','meaning':'Isolation Forest identifica observaciones para revisión. Su umbral configurado no demuestra fraude ni un aumento del riesgo respecto a otro periodo.'})
    profiles=snapshot['profiles']
    if profiles['negative_capacity']:
        alerts.append({'level':'Atención','title':f"{int(profiles['negative_capacity']):,} perfiles con capacidad estimada negativa",'meaning':'Se trata del portafolio sintético de Dataset 3 en USD. Su fórmula descuenta la cuota del préstamo bajo el supuesto declarado de la fuente.'})
    return {'income':income,'expense':expense,'net':net,'expense_change_pct':growth,'customers':total,
            'anomalies':anomalies,'anomaly_rate':100*anomalies/total if total else 0.,'alerts':alerts}
