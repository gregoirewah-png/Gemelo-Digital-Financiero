"""Vista de negocio e ingeniería. Protegida por credenciales de operación."""
import hmac
import os
import sys
from pathlib import Path

sys.path.insert(0,str(Path(__file__).resolve().parents[2]))
import pandas as pd
import plotly.express as px
import plotly.graph_objects as go
import streamlit as st

from src.common.config import required, redact
from src.dashboard.ui import configure, load_frame
from src.ml.monte_carlo import MonteCarloConfig

configure('Control financiero y operación','▥')
st.caption('GEMELO DIGITAL FINANCIERO · NEGOCIO E INGENIERÍA')
st.title('Visión del portafolio y de la plataforma')
st.caption(f"Versión instalada · {os.getenv('GEMELO_VERSION', 'desarrollo')}")
if not st.session_state.get('operations_authenticated'):
    with st.form('login'):
        st.subheader('Acceso del equipo')
        user = st.text_input('Usuario')
        password = st.text_input('Contraseña',type='password')
        submit = st.form_submit_button('Ingresar',type='primary')
    if submit:
        valid = (hmac.compare_digest(user.encode(),required('OPERATIONS_USER').encode())
                 and hmac.compare_digest(password.encode(),required('OPERATIONS_PASSWORD').encode()))
        if valid:
            st.session_state.operations_authenticated = True
            st.rerun()
        st.error('Credenciales incorrectas')
    st.stop()

left,right = st.columns([6,1])
with right:
    if st.button('Actualizar'):
        load_frame.clear()
    if st.button('Salir'):
        st.session_state.operations_authenticated = False
        st.rerun()

try:
    jobs = load_frame('SELECT * FROM ops.job_runs ORDER BY started_at DESC LIMIT 10000')
    publication = load_frame('SELECT * FROM serving.publication')
except Exception as exc:
    st.error('No se pudo leer PostgreSQL. ' + redact(str(exc)))
    st.stop()

business,quality,technical,scenarios,personal_tab,risk_tab = st.tabs(['Panorama financiero','Calidad y operación','Detalle técnico','Escenarios','Perfil personal','Modelo de impago'])
with business:
    if publication.empty:
        st.info('Gold aún no está publicado. Ejecuta el DAG maestro y consulta el estado en Calidad y operación.')
    else:
        pub = publication.iloc[0]
        st.caption(f"Publicación {pub.run_id} · Disponible desde {pub.published_at}")
        monthly = load_frame('SELECT * FROM serving.monthly_cashflow ORDER BY period_month')
        categories = load_frame('SELECT category,SUM(expense_total) AS expense_total FROM serving.category_spending GROUP BY category ORDER BY expense_total DESC')
        cards = load_frame('SELECT cluster,COUNT(*) AS customers,AVG(credit_utilization_pct) AS utilization,COUNT(*) FILTER(WHERE is_anomaly) AS anomalies FROM serving.credit_cards GROUP BY cluster ORDER BY cluster')
        profiles = load_frame('SELECT COUNT(*) AS profiles,AVG(savings_capacity) AS average_capacity FROM serving.financial_profiles')
        cols = st.columns(4)
        cols[0].metric('Ingresos históricos · D1',f"{float(monthly.total_income.sum()):,.2f}")
        cols[1].metric('Flujo neto histórico · D1',f"{float(monthly.net_cash_flow.sum()):,.2f}")
        cols[2].metric('Clientes de tarjetas · D2',f"{int(cards.customers.sum()):,}")
        cols[3].metric('Perfiles financieros · D3',f"{int(profiles.iloc[0].profiles):,}")
        st.caption('D1 conserva la unidad monetaria de origen. D2 y D3 son poblaciones independientes. El flujo histórico no equivale a un saldo de cuenta.')
        melted = monthly.melt(id_vars=['period_month'],value_vars=['total_income','total_expenses','net_cash_flow'],var_name='Indicador',value_name='Importe')
        st.plotly_chart(px.line(melted,x='period_month',y='Importe',color='Indicador',title='Ingresos, gastos y flujo mensual · Dataset 1',color_discrete_sequence=['#1464a5','#f7893b','#2dcccd']),use_container_width=True)
        col1,col2 = st.columns(2)
        with col1:
            st.plotly_chart(px.bar(categories,x='expense_total',y='category',orientation='h',title='Composición del gasto · Dataset 1'),use_container_width=True)
        with col2:
            st.plotly_chart(px.bar(cards,x='cluster',y='customers',color='cluster',title='Segmentos de tarjetas · Dataset 2'),use_container_width=True)
            st.caption(f"Anomalías detectadas {int(cards.anomalies.sum()):,}. Son observaciones atípicas del modelo.")

with quality:
    if jobs.empty:
        st.info('Todavía no hay ejecuciones registradas.')
    else:
        # Los reintentos previos se conservan para auditoría y se excluyen del resumen.
        latest = jobs.sort_values('attempt').drop_duplicates(['run_id','task_id'],keep='last').copy()
        runs = ['Todas']+latest.sort_values('started_at',ascending=False).run_id.unique().tolist()
        default_run = publication.iloc[0].run_id if not publication.empty else 'Todas'
        run = st.selectbox('Corrida',runs,index=runs.index(default_run) if default_run in runs else 0)
        selection = latest if run=='Todas' else latest[latest.run_id==run]
        metrics = pd.json_normalize(selection.metrics).fillna(0)
        counts = pd.concat([selection.reset_index(drop=True),metrics.reset_index(drop=True)],axis=1)
        silver = counts[counts.layer=='silver']
        def total(column):
            return float(silver[column].sum()) if column in silver else 0
        read, rejected = total('records_read'),total('records_quarantined')
        cols = st.columns(4)
        cols[0].metric('Tareas exitosas',f"{100*(selection.status=='SUCCESS').mean():.1f}%")
        cols[1].metric('Registros evaluados · Silver',f'{int(read):,}')
        cols[2].metric('Registros rechazados · Silver',f'{100*rejected/read:.2f}%' if read else 'Sin datos')
        cols[3].metric('Tareas fallidas',int((selection.status=='FAILED').sum()))
        st.caption('Rechazo = registros en cuarentena / registros leídos en Silver. Duplicados y reglas fallidas se contabilizan por separado. Se usa el último intento de cada tarea.')
        selection = selection.copy()
        selection['day'] = pd.to_datetime(selection.started_at,utc=True).dt.date
        st.plotly_chart(px.bar(selection.groupby(['day','status']).size().reset_index(name='tasks'),x='day',y='tasks',color='status',title='Estado diario de las tareas',color_discrete_map={'SUCCESS':'#028484','FAILED':'#da3851','RUNNING':'#e9ab35'}),use_container_width=True)
        if not silver.empty:
            for column in ['records_read','records_written','records_quarantined','duplicates_removed']:
                if column not in silver:
                    silver = silver.assign(**{column:0})
            grouped = silver.groupby('dataset')[['records_written','records_quarantined','duplicates_removed']].sum().reset_index()
            st.plotly_chart(px.bar(grouped.melt(id_vars='dataset',var_name='Estado',value_name='Registros'),x='dataset',y='Registros',color='Estado',title='Destino de los registros Silver'),use_container_width=True)
        st.plotly_chart(px.bar(selection,x='task_id',y='duration_seconds',color='layer',title='Duración de tareas en segundos'),use_container_width=True)
        st.dataframe(selection[['run_id','task_id','dataset','status','duration_seconds','started_at']],use_container_width=True,hide_index=True)

with technical:
    st.subheader('Trazabilidad y logs por intento')
    if not jobs.empty:
        dataset = st.selectbox('Fuente técnica',['Todas']+sorted(jobs.dataset.unique().tolist()))
        filtered = jobs if dataset=='Todas' else jobs[jobs.dataset==dataset]
        selected = st.selectbox('Tarea e intento',filtered.index.tolist(),format_func=lambda i:f"{jobs.loc[i,'run_id']} · {jobs.loc[i,'task_id']} · intento {jobs.loc[i,'attempt']} · {jobs.loc[i,'status']}")
        row = jobs.loc[selected]
        st.json(row.metrics)
        if row.error:
            st.error(row.error)
        st.code(row.log_content or 'Sin log almacenado para este intento.',language='text')
    if not publication.empty:
        st.subheader('Validación dimensional y Feature Store')
        st.json(publication.iloc[0].manifest)

with scenarios:
    st.subheader('Monte Carlo · Dataset 1')
    st.caption('Remuestreo mensual pareado. Sin estacionalidad, inflación, intereses ni tendencia. Percentiles de simulación sujetos a la cobertura histórica.')
    if not publication.empty:
        with st.form('simulation'):
            c1,c2,c3 = st.columns(3)
            initial = c1.number_input('Saldo inicial supuesto',min_value=0.,max_value=1e12,value=0.)
            months = c2.number_input('Meses',1,60,12)
            seed = c3.number_input('Semilla',0,4294967295,42)
            simulations = st.number_input('Trayectorias por escenario',100,50000,5000,100)
            expense_change = st.slider('Cambio porcentual de gasto del escenario ajustado',-100,200,10)
            extra = st.number_input('Gasto mensual adicional del escenario ajustado',min_value=0.,max_value=1e9,value=0.)
            simulate_now = st.form_submit_button('Calcular y publicar simulación',type='primary')
        if simulate_now:
            from src.analytics.run_monte_carlo import run_simulation
            config = MonteCarloConfig(initial_savings=initial,months=int(months),simulations=int(simulations),seed=int(seed),scenarios=[
                {'name':'Base','income_change_pct':0.,'expense_change_pct':0.,'extra_monthly_expense':0.},
                {'name':'Ajustado','income_change_pct':0.,'expense_change_pct':float(expense_change),'extra_monthly_expense':extra}])
            try:
                with st.spinner('Simulando y publicando…'):
                    result = run_simulation(configuration=config)
                st.success('Simulación publicada ' + result['simulation_run_id'])
                load_frame.clear()
            except Exception as exc:
                st.error(redact(str(exc)))
    runs = load_frame('SELECT * FROM simulation.runs ORDER BY created_at DESC LIMIT 100')
    if not runs.empty:
        simulation_id = st.selectbox('Simulación publicada',runs.simulation_run_id.tolist())
        selected_run = runs[runs.simulation_run_id==simulation_id].iloc[0]
        st.caption(f"Fuente Gold {selected_run.source_run_id} · {selected_run.historical_months} meses históricos")
        results = load_frame('SELECT * FROM simulation.results WHERE simulation_run_id=%s ORDER BY scenario,month',(simulation_id,))
        scenario = st.selectbox('Escenario',results.scenario.unique().tolist())
        chart = results[results.scenario==scenario]
        fig = go.Figure()
        fig.add_trace(go.Scatter(x=chart.period_month,y=chart.p95,name='P95',line_color='#a5d9ee'))
        fig.add_trace(go.Scatter(x=chart.period_month,y=chart.p05,name='P05',fill='tonexty',fillcolor='rgba(20,100,165,.12)',line_color='#a5d9ee'))
        fig.add_trace(go.Scatter(x=chart.period_month,y=chart.p50,name='P50',line_color='#043263'))
        fig.update_layout(title='Saldo acumulado simulado',xaxis_title='Mes',yaxis_title='Unidad monetaria de Dataset 1')
        st.plotly_chart(fig,use_container_width=True)
        final = chart.iloc[-1]
        c1,c2,c3 = st.columns(3)
        c1.metric('Saldo mediano final',f'{final.p50:,.2f}')
        c2.metric('Saldo negativo al final',f'{100*final.probability_negative:.1f}%')
        c3.metric('Saldo negativo en algún mes',f'{100*final.probability_ever_negative:.1f}%')
        st.caption('La banda P05–P95 contiene el 90% central de los saldos simulados en cada mes. No es una garantía ni un intervalo de confianza estadístico. Un saldo negativo representa déficit acumulado bajo estos supuestos.')
        st.json(selected_run.configuration)

with personal_tab:
    st.subheader('Gemelo personal de demostración')
    st.caption('Identidad común, movimientos, ahorros, préstamo y tarjeta conciliados. Persona ficticia independiente de los tres portafolios públicos.')
    try:
        from src.analytics.personal_finance import personal_snapshot
        snapshot=personal_snapshot()
        person=snapshot['profile']
        columns=st.columns(4)
        columns[0].metric('Persona',person['display_name'])
        columns[1].metric('Disponible',f"${person['available_savings']:,.0f}")
        columns[2].metric('Margen mensual promedio',f"${person['monthly_available']:,.0f}")
        columns[3].metric('Modelo de riesgo',person['credit_risk']['level'])
        st.dataframe(pd.DataFrame(snapshot['monthly']),use_container_width=True,hide_index=True)
        st.json({'profile_id':person['profile_id'],'source_run_id':snapshot['publication']['run_id'],
                 'source_kind':person['data_kind'],'expense_definition':person['expense_definition'],
                 'risk':person['credit_risk']})
    except Exception:
        st.info('El perfil se publicará cuando concluya la nueva corrida integral.')

with risk_tab:
    st.subheader('Probabilidad de impago · Modelo supervisado')
    models=load_frame('SELECT * FROM serving.credit_default_models')
    if models.empty:
        st.info('El entrenamiento y la validación se publicarán con la nueva corrida.')
    else:
        metrics=models.iloc[0].metrics
        columns=st.columns(4)
        columns[0].metric('ROC-AUC de prueba',f"{metrics['roc_auc']:.3f}")
        columns[1].metric('Brier de prueba',f"{metrics['brier_score']:.3f}")
        columns[2].metric('Brier de referencia',f"{metrics['baseline_brier']:.3f}")
        columns[3].metric('Registros de prueba',f"{metrics['test_rows']:,}")
        st.caption('XGBoost calibrado con un conjunto separado. La prueba final permanece fuera del entrenamiento y la calibración. Las variables protegidas no se incluyen en el modelo.')
        calibration=pd.DataFrame(metrics['calibration'])
        figure=px.scatter(calibration,x='predicted',y='observed',labels={'predicted':'Probabilidad estimada','observed':'Frecuencia observada'},title='Calibración en el conjunto de prueba')
        figure.add_trace(go.Scatter(x=[0,1],y=[0,1],mode='lines',name='Referencia ideal',line_dash='dash'))
        st.plotly_chart(figure,use_container_width=True)
        st.json({key:metrics[key] for key in ['model_version','split_method','train_rows','calibration_rows','test_rows','average_precision','feature_names','limitations']})
        st.markdown('Fuente pública [UCI · Default of Credit Card Clients](https://archive.ics.uci.edu/dataset/350/default+of+credit+card+clients). Licencia CC BY 4.0. Cohorte de Taiwán de 2005.')
