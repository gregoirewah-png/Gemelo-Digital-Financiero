"""Qué está pasando y qué significa para el negocio."""
import sys
from pathlib import Path
sys.path.insert(0,str(Path(__file__).resolve().parents[2]))
import pandas as pd
import plotly.express as px
import streamlit as st
from src.analytics.executive_metrics import executive_snapshot,business_findings
from src.dashboard.ui import configure
from src.dashboard.access import require_team_login

configure('Visión ejecutiva','◈')
st.caption('GEMELO DIGITAL FINANCIERO · DIRECCIÓN')
st.title('La evolución del negocio, en contexto')
st.write('Tendencias, señales que merecen atención y resultados de los modelos.')
require_team_login('executive')
@st.cache_data(ttl=30,show_spinner=False)
def load_business():return executive_snapshot()
left,right=st.columns([5,1])
with right:
    if st.button('Actualizar'):load_business.clear()
    if st.button('Salir'):
        st.session_state.executive_authenticated=False;st.rerun()
try:data=load_business()
except Exception:
    st.info('El resumen estará disponible cuando se confirme una publicación analítica.');st.stop()
findings=business_findings(data)
monthly=pd.DataFrame(data['monthly'])
period=pd.Timestamp(monthly.iloc[-1].period_month).strftime('%m/%Y')
st.caption(f'Último mes de transacciones: {period}. Las fuentes son portafolios independientes; sus importes no se suman entre sí.')
a,b,c,d=st.columns(4)
a.metric('Ingresos del último mes · D1',f"{findings['income']:,.0f}")
b.metric('Flujo del último mes · D1',f"{findings['net']:,.0f}")
c.metric('Cambio del gasto mensual',f"{findings['expense_change_pct']:+.1f}%" if findings['expense_change_pct'] is not None else 'Sin comparación')
d.metric('Tarjetas para revisión · D2',f"{findings['anomalies']:,}")
trend,alerts,models=st.tabs(['Tendencias','Alertas y segmentos','Modelos y escenarios'])
with trend:
    st.subheader('Ingresos, gastos y margen a lo largo del tiempo')
    scope=st.radio('Periodo de análisis',['Últimos 12 meses','Historia completa'],horizontal=True)
    selected=monthly.tail(12) if scope=='Últimos 12 meses' else monthly
    plotted=selected.rename(columns={'total_income':'Ingresos','total_expenses':'Gastos','net_cash_flow':'Flujo neto'}).melt(id_vars='period_month',var_name='Indicador',value_name='Importe')
    st.plotly_chart(px.line(plotted,x='period_month',y='Importe',color='Indicador',color_discrete_sequence=['#148b90','#db8751','#1464a5'],labels={'period_month':'Mes'}),use_container_width=True)
    st.write('El flujo muestra la diferencia entre entradas y salidas del mes. Permite reconocer faltantes recurrentes y meses con mayor margen dentro de la cobertura disponible.')
    categories=pd.DataFrame(data['categories'])
    st.plotly_chart(px.bar(categories.head(8).sort_values('expense_total'),x='expense_total',y='category',orientation='h',labels={'expense_total':'Gasto histórico · unidad de D1','category':''},title='Rubros que concentran el gasto histórico'),use_container_width=True)
    st.caption('Dataset 1 no confirma su moneda ni el saldo real de una cuenta. El último dato histórico no debe presentarse como un dato del mes actual.')
with alerts:
    for alert in findings['alerts']:
        st.subheader(alert['title'])
        st.write(alert['meaning'])
    segments=pd.DataFrame(data['segments'])
    segments['Segmento']=segments.cluster.map(lambda value:f'Segmento {value}')
    st.plotly_chart(px.bar(segments,x='Segmento',y='customers',color='Segmento',labels={'customers':'Clientes'},title='Distribución de los perfiles de tarjetas'),use_container_width=True)
    st.caption(f"Se revisan {findings['anomaly_rate']:.2f}% de {findings['customers']:,} tarjetas con el umbral del modelo. Los segmentos son agrupaciones estadísticas, sin jerarquía comercial automática.")
with models:
    st.subheader('Lo que aporta cada modelo')
    st.write('K-Means organiza patrones de uso. Isolation Forest prioriza observaciones atípicas. XGBoost estima impago dentro de su cohorte de evaluación. Monte Carlo compara escenarios de flujo.')
    metrics=data['model']
    if metrics:
        a,b,c=st.columns(3)
        a.metric('Discriminación en prueba · AUC',f"{metrics['roc_auc']:.3f}")
        b.metric('Error probabilístico · Brier',f"{metrics['brier_score']:.3f}")
        c.metric('Referencia de Brier',f"{metrics['baseline_brier']:.3f}")
        st.write('Un Brier menor es mejor. La referencia permite juzgar si el modelo mejora una estimación constante. Estos resultados corresponden a UCI, Taiwán 2005; no validan una decisión bancaria con clientes actuales.')
    if data['results']:
        st.subheader('Escenarios del perfil ficticio')
        results=pd.DataFrame(data['results'])
        st.plotly_chart(px.line(results,x='period_month',y='p50',color='scenario',labels={'period_month':'Mes futuro','p50':'Saldo central · pesos de demostración','scenario':'Escenario'}),use_container_width=True)
        st.caption('Alex es ficticio. Las curvas son medianas de posibilidades simuladas bajo los supuestos guardados. No garantizan resultados ni representan el portafolio completo.')
    st.caption('Las cifras y modelos de esta pantalla pertenecen a la misma publicación confirmada. Los logs y el diagnóstico de ejecución están en la vista de Data Engineering.')
