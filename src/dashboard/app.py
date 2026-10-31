import streamlit as st
import pandas as pd
import psycopg2
import sys
import os

# --- CONFIGURACIÓN DE RUTAS ---
# Directorio actual (src/dashboard)
CURRENT_DIR = os.path.dirname(os.path.abspath(__file__))

# Ruta hacia la carpeta hermana src/ml
ML_DIR = os.path.abspath(os.path.join(CURRENT_DIR, "..", "ml"))

# Agregar src/ml al path de Python si no está
if ML_DIR not in sys.path:
    sys.path.append(ML_DIR)

# Importar el agente desde src/ml/agent_service.py
from agent_service import agente_financiero

st.set_page_config(
    page_title="Gemelo Digital Financiero | BBVA",
    page_icon="🤖",
    layout="wide"
)
st.set_page_config(
    page_title="Gemelo Digital Financiero | BBVA",
    page_icon="🤖",
    layout="wide"
)

# --- CREACIÓN DE PESTAÑAS PRINCIPALES ---
tab_dashboard, tab_chat = st.tabs(["📊 Dashboard de Calidad", "💬 Asistente IA (Strands Agent)"])

# ==========================================
# PESTAÑA 1: EL DASHBOARD QUE YA TENÍAS
# ==========================================
with tab_dashboard:
    @st.cache_resource
    def init_connection():
        return psycopg2.connect(
            dbname="gemelo_db",
            user="admin",
            password="admin",
            host="postgres",
            port="5432"
        )

    @st.cache_data(ttl=60)
    def load_logs_from_postgres():
        try:
            conn = init_connection()
            query = "SELECT * FROM execution_logs ORDER BY created_at DESC;"
            df = pd.read_sql(query, conn)
            return df
        except Exception as e:
            st.error(f"Error al conectar con la base de datos: {e}")
            return pd.DataFrame()

    st.title("📊 Monitor de Calidad de Datos (PostgreSQL & Medallion)")
    st.markdown("Auditoría centralizada de ejecuciones y validaciones de **Great Expectations**.")

    df_logs = load_logs_from_postgres()

    if df_logs.empty:
        st.info("Aún no hay registros en la tabla `execution_logs` de PostgreSQL. Ejecuta tus pipelines en Airflow para poblar la información.")
    else:
        # --- FILTROS EN SIDEBAR (Solo actúan en el dashboard) ---
        st.sidebar.header("Filtros de Auditoría")
        datasets_disponibles = sorted(list(df_logs["dataset_name"].unique()))
        selected_dataset = st.sidebar.selectbox("Seleccionar Dataset", ["Todos"] + datasets_disponibles)

        filtered_df = (
            df_logs if selected_dataset == "Todos"
            else df_logs[df_logs["dataset_name"] == selected_dataset]
        )

        if not filtered_df.empty:
            opciones = [
                f"{row['dataset_name']} | Run: {row['run_id'][:16]}... | {row['created_at']}"
                for _, row in filtered_df.iterrows()
            ]
            seleccion = st.sidebar.selectbox("Seleccionar Corrida", opciones)
            
            selected_idx = opciones.index(seleccion)
            selected_log = filtered_df.iloc[selected_idx]

            st.subheader(f"Ejecución: {selected_log['dataset_name']}")
            st.caption(
                f"**Run ID:** `{selected_log['run_id']}` | "
                f"**Fecha de Registro:** {selected_log['created_at']}"
            )
            st.caption(f"⏱️ **Duración del proceso:** {selected_log['duration_seconds']} s")

            st.markdown("### 📦 Flujo de Registros")
            col1, col2, col3, col4 = st.columns(4)
            col1.metric("Leídos (Bronze)", selected_log["records_read"])
            col2.metric("Duplicados Eliminados", selected_log["duplicates_removed"])
            col3.metric("Cuarentena", selected_log["records_quarantined"])
            col4.metric("Aprobados (Silver)", selected_log["records_validated"])

            st.markdown("---")
            st.markdown("### 🛡️ Reglas de Calidad (Great Expectations)")
            evaluadas = int(selected_log["expectations_evaluated"])
            fallidas = int(selected_log["expectations_failed"])
            exitosas = evaluadas - fallidas

            c1, c2, c3 = st.columns(3)
            c1.metric("Reglas Evaluadas", evaluadas)
            c2.metric("Reglas Exitosas", exitosas)
            c3.metric(
                "Reglas Fallidas",
                fallidas,
                delta=-fallidas if fallidas > 0 else 0,
                delta_color="inverse"
            )

            with st.expander("Ver Log Completo de la Ejecución"):
                st.code(selected_log["log_content"], language="text")

# ==========================================
# PESTAÑA 2: EL ASISTENTE IA CON STRANDS
# ==========================================
with tab_chat:
    st.title("🤖 Asistente Inteligente - Gemelo Digital Financiero")
    st.markdown("Pregúntale al agente sobre saldos, transacciones, segmentos de clientes o métricas analíticas de la capa Gold.")

    # Inicializar el historial de chat
    if "messages" not in st.session_state:
        st.session_state.messages = []

    # Mostrar mensajes anteriores
    for message in st.session_state.messages:
        with st.chat_message(message["role"]):
            st.markdown(message["content"])

    # Capturar la entrada del usuario en el chat
    if prompt := st.chat_input("Ej: ¿Cómo están distribuidos los clientes en los segmentos de K-Means?"):
        st.session_state.messages.append({"role": "user", "content": prompt})
        with st.chat_message("user"):
            st.markdown(prompt)

        with st.chat_message("assistant"):
            with st.spinner("El agente está analizando la base de datos Gold..."):
                try:
                    # Invocamos al agente de Strands que consultará PostgreSQL si lo requiere
                    response = agente_financiero.generar_respuesta(prompt)
                    st.markdown(response)
                except Exception as e:
                    response = f"Ocurrió un error al procesar tu solicitud con el agente: {e}"
                    st.error(response)
                    
        st.session_state.messages.append({"role": "assistant", "content": response})