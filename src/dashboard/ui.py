"""Estilo común y lectura SQL para las tres experiencias."""
import pandas as pd
import streamlit as st

from src.common.config import db_connect


def configure(title, icon='◈'):
    st.set_page_config(page_title=title,page_icon=icon,layout='wide')
    st.markdown('''<style>
    .stApp {
        background:var(--background-color, #f3f6fb);
        color:var(--text-color, #122b4a);
    }
    .block-container {padding-top:2.2rem;max-width:1450px;}
    h1,h2,h3 {color:var(--text-color, #122b4a);letter-spacing:-.02em;}
    [data-testid="stMetric"], [data-testid="stChatMessage"] {
        background:var(--secondary-background-color, #ffffff);
        color:var(--text-color, #122b4a);
        border:1px solid rgba(128,128,128,.25);
        border-radius:14px;
    }
    [data-testid="stMetric"] {padding:18px;}
    [data-testid="stMetricValue"], [data-testid="stMetricLabel"],
    [data-testid="stChatMessageContent"], [data-testid="stCaptionContainer"] {
        color:var(--text-color, #122b4a);
    }
    div.stButton>button[kind="primary"] {
        background:#1464a5;border-color:#1464a5;color:#ffffff;
    }
    </style>''',unsafe_allow_html=True)


@st.cache_data(ttl=20,show_spinner=False)
def load_frame(query, params=(), reader=False):
    with db_connect(reader=reader) as conn, conn.cursor() as cursor:
        cursor.execute(query,params)
        return pd.DataFrame(cursor.fetchall(),columns=[column.name for column in cursor.description])
