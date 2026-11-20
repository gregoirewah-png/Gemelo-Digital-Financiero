"""Qué ocurrió con los datos: ejecución, fallos, contratos y trazabilidad."""
import os
import sys
from pathlib import Path
sys.path.insert(0,str(Path(__file__).resolve().parents[2]))
import pandas as pd
import plotly.express as px
import streamlit as st
from src.common.config import redact
from src.dashboard.ui import configure,load_frame
from src.dashboard.access import require_team_login

configure('Data Engineering','▥')
st.caption('GEMELO DIGITAL FINANCIERO · DATA ENGINEERING')
st.title('Ejecución y trazabilidad de los datos')
st.write('Qué se procesó, qué falló y qué publicación puede consumirse.')
st.caption(f"Versión instalada · {os.getenv('GEMELO_VERSION','desarrollo')}")
require_team_login('operations')
left,right=st.columns([5,1])
with right:
    if st.button('Actualizar'):load_frame.clear()
    if st.button('Salir'):
        st.session_state.operations_authenticated=False;st.rerun()
try:
    jobs=load_frame('SELECT * FROM ops.job_runs ORDER BY started_at DESC LIMIT 10000')
    publication=load_frame('SELECT * FROM serving.publication')
except Exception as exc:
    st.error('No se pudo leer PostgreSQL. '+redact(str(exc)));st.stop()
quality,technical,sources,knowledge=st.tabs(['Ejecuciones y calidad','Fallos y logs','Publicaciones y bases','Conocimiento RAG'])
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


with sources:
    st.subheader('Productos publicados y procedencia')
    if publication.empty:
        st.info('No hay una publicación confirmada.')
    else:
        st.write('Publicación activa',publication.iloc[0].run_id)
        inventory=load_frame("SELECT 'Flujo mensual D1' AS base,COUNT(*) AS filas FROM serving.monthly_cashflow UNION ALL SELECT 'Gasto por categoría D1',COUNT(*) FROM serving.category_spending UNION ALL SELECT 'Tarjetas D2',COUNT(*) FROM serving.credit_cards UNION ALL SELECT 'Features y scores Isolation Forest',COUNT(*) FROM serving.credit_card_anomalies UNION ALL SELECT 'Perfiles D3',COUNT(*) FROM serving.financial_profiles UNION ALL SELECT 'Identidad personal ficticia',COUNT(*) FROM serving.personal_profiles")
        st.dataframe(inventory,use_container_width=True,hide_index=True)
        st.markdown('**Base adicional de Isolation Forest:** producto derivado de Dataset 2. Su grano es publicación y cliente; conserva scores, variables de pago y procedencia. Una fila por cliente, incluso cuando no se marca como atípico.')
        preview=load_frame('SELECT cust_id,is_anomaly,anomaly_score,isolation_score,decision_score,model_run_id,scored_at FROM serving.credit_card_anomalies ORDER BY anomaly_score DESC LIMIT 20')
        st.dataframe(preview,use_container_width=True,hide_index=True)
        st.caption('El catálogo explica el origen, las columnas y los límites. Las métricas de negocio y la interpretación ejecutiva se consultan en la vista ejecutiva.')
with knowledge:
    st.subheader('Corpus curado y recuperación')
    try:
        corpus=load_frame('SELECT c.* FROM knowledge.corpora c JOIN knowledge.active_corpus a ON c.version=a.version')
        docs=load_frame('SELECT d.document_id,d.title,d.publisher,d.source_url,d.reviewed_at,d.content_hash FROM knowledge.documents d JOIN knowledge.active_corpus a ON d.corpus_version=a.version ORDER BY d.document_id')
        if corpus.empty:st.info('El corpus se prepara en la inicialización de la plataforma.')
        else:
            st.json(corpus.iloc[0].to_dict())
            st.dataframe(docs,use_container_width=True,hide_index=True)
            st.caption('Recuperación TF-IDF y coseno. Las guías aportan conceptos y límites; las cifras personales proceden de Gold. El corpus se versiona por contenido.')
    except Exception as exc:
        st.info('Conocimiento todavía no inicializado. '+redact(str(exc)))
