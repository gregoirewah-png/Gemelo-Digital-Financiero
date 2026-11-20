"""Experiencia personal de presentación, centrada en conversar y entender el dinero."""
import html
import os
import re
import sys
from pathlib import Path

sys.path.insert(0,str(Path(__file__).resolve().parents[2]))
import pandas as pd
import plotly.express as px
import plotly.graph_objects as go
import streamlit as st

from src.analytics.personal_finance import personal_snapshot
from src.dashboard.ui import configure
from src.dashboard.access import require_demo_access
from src.ml.personal_agent import PersonalFinancialAgent, money, month_label

configure('Mi futuro financiero','🌿')
require_demo_access()
st.markdown('''<style>
.block-container {max-width:1030px;padding-top:1rem;padding-bottom:calc(2rem + env(safe-area-inset-bottom));}
.month-cards {display:grid;grid-template-columns:repeat(3,minmax(0,1fr));gap:.8rem;margin:.7rem 0 1rem;}
.month-card {background:#fff;border:1px solid #dce5ee;border-radius:16px;padding:1.1rem;color:#122b4a;}
.month-card span {display:block;color:#50667c;font-size:.8rem;margin-bottom:.45rem;}
.month-card strong {font-size:clamp(1.1rem,3.8vw,1.9rem);white-space:nowrap;}
[data-testid="stChatInput"] textarea {font-size:16px!important;}
[data-testid="stChatInput"] {padding-bottom:env(safe-area-inset-bottom);}
.stDeployButton, [data-testid="stAppDeployButton"] {display:none;}
.personal-brand {display:flex;justify-content:space-between;align-items:center;gap:1rem;margin-bottom:1.5rem;}
.personal-brand strong {color:#043263;letter-spacing:.02em;font-size:1rem;}
.demo-badge {background:#e8f2fb;color:#245378;border-radius:999px;padding:.4rem .8rem;font-size:.72rem;letter-spacing:.05em;}
.personal-hero {position:relative;overflow:hidden;background:linear-gradient(120deg,#043263 0%,#145e93 63%,#148b90 100%);border-radius:24px;padding:2.3rem 2.5rem;margin-bottom:1.6rem;box-shadow:0 12px 30px rgba(4,50,99,.13);}
.personal-hero:after {content:'';position:absolute;right:-40px;top:-85px;width:300px;height:300px;border:45px solid rgba(255,255,255,.07);border-radius:50%;pointer-events:none;}
.personal-hero .eyebrow {color:#a9ece8;font-size:.8rem;letter-spacing:.08em;font-weight:600;}
.personal-hero h1 {color:white!important;font-size:clamp(1.8rem,4vw,2.75rem);line-height:1.12;margin:.6rem 0 .75rem;max-width:700px;font-weight:700;}
.personal-hero p {color:#e0edf7;font-size:1.02rem;line-height:1.6;max-width:640px;margin:0;}
.balance-note {display:flex;justify-content:space-between;align-items:center;flex-wrap:wrap;gap:1rem;background:#e8f5f2;border:1px solid #cfebe4;border-radius:15px;padding:1rem 1.3rem;margin:.6rem 0 1.7rem;color:#19564d;}
.balance-note strong {font-size:1.15rem;}
.conversation-intro {margin-top:.3rem;margin-bottom:.65rem;}
.conversation-intro h2 {font-size:1.4rem;margin-bottom:.3rem;color:#122b4a;}
.conversation-intro p {color:#4d647c;margin:0;font-size:.95rem;}
[data-testid="stChatMessage"] {padding:1rem 1.15rem;margin-top:.6rem;box-shadow:0 3px 12px rgba(18,43,74,.025);}
[data-testid="stMetric"] {border-radius:17px;box-shadow:0 4px 16px rgba(18,43,74,.035);padding:1.25rem;}
[data-testid="stMetricLabel"] {font-size:.9rem;}
[data-testid="stMetricValue"] {font-size:1.85rem;font-weight:650;}
@media(max-width:640px) {
.block-container {padding: .6rem 1rem calc(1.5rem + env(safe-area-inset-bottom));}
.personal-brand {margin-bottom:.8rem;gap:.4rem;}.personal-brand strong {font-size:.77rem;}.demo-badge {font-size:.56rem;padding:.3rem .45rem;}
.personal-hero {padding:1.25rem;border-radius:20px;margin-bottom:.8rem;}.personal-hero h1 {font-size:1.7rem!important;line-height:1.1;}.personal-hero p {font-size:.88rem;line-height:1.45;}
.personal-hero:after {width:210px;height:210px;border-width:30px;right:-85px;top:-65px;}
.month-cards {gap:.45rem;}.month-card {padding:.8rem .55rem;border-radius:13px;}.month-card span {font-size:.65rem;}.month-card strong {font-size:1.14rem;}
.balance-note {padding:.85rem;margin-bottom:1rem;gap:.45rem;}.balance-note strong {font-size:1.05rem;}.balance-note span:last-child {font-size:.8rem;}
.conversation-intro h2 {font-size:1.18rem;}.conversation-intro p {font-size:.84rem;}
[data-testid="stHorizontalBlock"] {flex-wrap:nowrap!important;gap:.6rem!important;}
[data-testid="stColumn"] {min-width:0!important;flex:1 1 0!important;}
[data-testid="stButton"] button {min-height:48px;font-size:.83rem;padding:.5rem .4rem;}
[data-testid="stChatMessage"] {padding:.8rem;margin-top:.4rem;}
[data-testid="stChatMessageContent"] p {font-size:.94rem;}
}
</style>''',unsafe_allow_html=True)
st.markdown('<div class="personal-brand"><strong>🌿 MI FUTURO FINANCIERO</strong><span class="demo-badge">ESPACIO DE DEMOSTRACIÓN</span></div>',unsafe_allow_html=True)


@st.cache_data(ttl=20,show_spinner=False)
def load_profile():
    return personal_snapshot()


def render_result(result):
    if not result:
        return
    if result['kind']=='knowledge':
        with st.expander('Fuentes de esta explicación'):
            for source in result.get('sources',[]):
                label=source['publisher']+' · '+source['title']
                if source.get('source_url'):
                    st.markdown('['+label+']('+source['source_url']+')')
                else:st.caption(label)
        return
    if result['kind']=='categories':
        frame=pd.DataFrame(result['data'])
        frame['expense_total']=frame.expense_total.astype(float)
        figure=px.bar(frame.sort_values('expense_total'),x='expense_total',y='category',orientation='h',
                      color_discrete_sequence=['#148b90'],labels={'expense_total':'Pesos','category':''})
        figure.update_layout(height=300,margin=dict(l=0,r=15,t=15,b=0),paper_bgcolor='rgba(0,0,0,0)',
                             plot_bgcolor='rgba(0,0,0,0)',font_color='#24415f')
        st.plotly_chart(figure,use_container_width=True,config={'displayModeBar':False})
    if result['kind']=='simulation':
        frame=pd.DataFrame(result['data']['results'])
        names=list(dict.fromkeys(frame.scenario))
        band=frame[frame.scenario==names[-1]].sort_values('month')
        figure=go.Figure()
        figure.add_trace(go.Scatter(x=band.period_month,y=band.p95,mode='lines',line_width=0,showlegend=False,hoverinfo='skip'))
        figure.add_trace(go.Scatter(x=band.period_month,y=band.p05,mode='lines',line_width=0,fill='tonexty',
                                   fillcolor='rgba(20,139,144,.13)',name='Rango orientativo',hoverinfo='skip'))
        for index,name in enumerate(names):
            selected=frame[frame.scenario==name].sort_values('month')
            figure.add_trace(go.Scatter(x=selected.period_month,y=selected.p50,name=name,mode='lines',
                                       line=dict(width=3,color=['#148b90','#1464a5','#dd9a35'][index%3]),
                                       hovertemplate='%{x|%b %Y} · $%{y:,.0f}<extra>%{fullData.name}</extra>'))
        figure.update_layout(height=330,margin=dict(l=0,r=10,t=20,b=0),legend=dict(orientation='h',y=1.15),
                             yaxis_tickprefix='$',yaxis_tickformat=',.0f',font_color='#24415f',
                             paper_bgcolor='rgba(0,0,0,0)',plot_bgcolor='rgba(0,0,0,0)',xaxis_title=None,yaxis_title='Saldo estimado')
        st.plotly_chart(figure,use_container_width=True,config={'displayModeBar':False})
        st.caption('Cada camino representa una posibilidad basada en tu historial de demostración. El saldo final puede variar.')


try:
    snapshot=load_profile()
except Exception:
    st.markdown('<div class="personal-hero"><div class="eyebrow">TU DINERO, CON MÁS CLARIDAD</div><h1>Estamos preparando tu espacio</h1><p>En unos momentos podrás revisar tus gastos y explorar tus próximos pasos.</p></div>',unsafe_allow_html=True)
    st.info('Tu perfil de demostración se está preparando. Actualiza la página en un momento.')
    st.stop()

profile=snapshot['profile']
name=html.escape(profile['display_name'])
last=profile['latest_month']
period=month_label(last['period_month'])
st.markdown(f'<div class="personal-hero"><div class="eyebrow">HOLA, {name.upper()}</div><h1>Tu dinero,<br>con más claridad.</h1><p>Entiende tu mes y prueba tu próxima decisión.</p></div>',unsafe_allow_html=True)
st.caption(f'Tu mes de {period} · Importes en pesos de demostración')
st.markdown('<div class="month-cards">'+''.join('<div class="month-card"><span>'+label+'</span><strong>'+money(last[key])+'</strong></div>' for label,key in [('Recibiste','total_income'),('Gastaste','total_expenses'),('Te quedó','net_cash_flow')])+'</div>',unsafe_allow_html=True)
st.markdown(f'<div class="balance-note"><span>💚 Tienes <strong>{money(profile["available_savings"])} disponibles</strong></span><span>Podemos ayudarte a preparar una meta o revisar una compra.</span></div>',unsafe_allow_html=True)
st.markdown('<div class="conversation-intro"><h2>¿Qué te gustaría entender hoy?</h2><p>Pregunta con tus palabras. Vamos paso a paso.</p></div>',unsafe_allow_html=True)

if 'messages' not in st.session_state:
    st.session_state.messages=[]
if 'agent' not in st.session_state:
    st.session_state.agent=PersonalFinancialAgent()

suggestions=[('🧭 Cómo voy','¿Cómo voy con mis finanzas?'),
             ('🛍️ Revisar una compra','¿Me alcanza para una compra de 15,000 pesos?'),
             ('🌱 Mi ahorro','¿Cuánto puedo ahorrar al mes?'),
             ('🔎 Mis gastos','¿En qué estoy gastando más?')]
selected_prompt=None
for offset in [0,2]:
    for column,(label,question) in zip(st.columns(2),suggestions[offset:offset+2]):
        if column.button(label,use_container_width=True):
            selected_prompt=question

if not st.session_state.messages:
    with st.chat_message('assistant',avatar='🌿'):
        st.write(f'Hola, {profile["display_name"]}. Vamos paso a paso. ¿Revisamos tus gastos, una meta o una compra?')
for message in st.session_state.messages:
    with st.chat_message(message['role'],avatar='🌿' if message['role']=='assistant' else None):
        st.markdown(re.sub(r'(?<!\\)\$',r'\$',message['content']))
        render_result(message.get('result'))

typed=st.chat_input('Por ejemplo, ¿qué pasa si gasto 2,000 pesos más al mes?',max_chars=4000)
prompt=typed or selected_prompt
if prompt and len(st.session_state.messages)//2>=int(os.getenv('DEMO_MAX_MESSAGES','30')):
    st.info('Has completado esta conversación de demostración. Puedes empezar una nueva desde la sección inferior.')
    prompt=None
if prompt:
    previous=[{'role':message['role'],'content':message['content']} for message in st.session_state.messages]
    st.session_state.messages.append({'role':'user','content':prompt})
    with st.chat_message('user'):
        st.markdown(prompt)
    with st.chat_message('assistant',avatar='🌿'):
        with st.spinner('Revisando tus números…'):
            answer=st.session_state.agent.generar_respuesta(prompt,history=previous)
        st.markdown(re.sub(r'(?<!\\)\$',r'\$',answer))
        result=st.session_state.agent.last_result
        render_result(result)
    st.session_state.messages.append({'role':'assistant','content':answer,'result':result})

with st.expander('Sobre este espacio de demostración'):
    st.write('Alex es una persona ficticia. Sus movimientos, ahorros, préstamo y tarjeta se generaron con una identidad común para mostrar cómo funciona el gemelo financiero. Los importes se expresan en pesos de demostración.')
    st.write('Las proyecciones ayudan a explorar posibilidades y dependen de los supuestos elegidos. Puedes pedir que te los explique con palabras sencillas.')
    if st.button('Empezar una nueva conversación'):
        st.session_state.messages=[]
        st.session_state.agent=PersonalFinancialAgent()
        st.rerun()

    if os.getenv('DEMO_REQUIRE_ACCESS','false').lower() in ['true','1','yes']:
        if st.button('Salir de la demostración'):
            st.session_state.clear()
            st.rerun()
