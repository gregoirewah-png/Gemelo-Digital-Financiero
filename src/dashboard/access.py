"""Acceso independiente para cada audiencia, con credenciales de entorno."""
import hmac
import os
import time
import streamlit as st
from src.common.config import required


def require_team_login(audience):
    prefix='EXECUTIVE' if audience=='executive' else 'OPERATIONS'
    state='executive_authenticated' if audience=='executive' else 'operations_authenticated'
    if st.session_state.get(state):return
    with st.form('login_'+audience):
        st.subheader('Acceso ejecutivo' if audience=='executive' else 'Acceso de Data Engineering')
        user=st.text_input('Usuario')
        password=st.text_input('Contraseña',type='password')
        submit=st.form_submit_button('Ingresar',type='primary')
    if submit:
        valid=hmac.compare_digest(user.encode(),required(prefix+'_USER').encode()) and hmac.compare_digest(password.encode(),required(prefix+'_PASSWORD').encode())
        if valid:
            st.session_state[state]=True;st.rerun()
        st.error('Credenciales incorrectas')
    st.stop()


def require_demo_access():
    """El servicio que se publica para iPhone siempre usa una clave de demostración."""
    if os.getenv('DEMO_REQUIRE_ACCESS','false').lower() not in ['true','1','yes']:return
    if st.session_state.get('demo_authenticated'):return
    st.markdown('### Tu espacio financiero')
    st.write('Introduce la clave de la demostración para continuar.')
    now=time.monotonic()
    failures=st.session_state.get('demo_failures',0)
    blocked_until=st.session_state.get('demo_blocked_until',0.)
    with st.form('demo_access'):
        code=st.text_input('Clave de acceso',type='password',max_chars=100)
        entered=st.form_submit_button('Entrar',type='primary',use_container_width=True)
    if entered:
        if now<blocked_until:
            st.warning('Espera un momento antes de volver a intentarlo.')
        elif hmac.compare_digest(code.encode(),required('DEMO_ACCESS_CODE').encode()):
            st.session_state.demo_authenticated=True
            st.session_state.demo_failures=0
            st.rerun()
        else:
            st.session_state.demo_failures=failures+1
            st.session_state.demo_blocked_until=now+min(60,2**min(failures,5))
            st.error('Revisa la clave de acceso.')
    st.stop()
