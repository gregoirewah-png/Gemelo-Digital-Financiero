import streamlit as st
import pandas as pd
import psycopg2
import re

st.set_page_config(
    page_title="Dashboard de Calidad | Airflow Medallion",
    page_icon="📊",
    layout="wide"
)

# --- CONEXIÓN A POSTGRESQL ---
@st.cache_resource
def init_connection():
    return psycopg2.connect(
        dbname="gemelo_db",
        user="admin",
        password="admin",
        host="postgres",
        port="5432"
    )

@st.cache_data(ttl=60) # Refresca los datos cada 60 segundos
def load_logs_from_postgres():
    try:
        conn = init_connection()
        query = "SELECT * FROM execution_logs ORDER BY created_at DESC;"
        df = pd.read_sql(query, conn)
        return df
    except Exception as e:
        st.error(f"Error al conectar con la base de datos: {e}")
        return pd.DataFrame()

# --- INTERFAZ ---

st.title("📊 Monitor de Calidad de Datos (PostgreSQL & Medallion)")
st.markdown("Auditoría centralizada de ejecuciones y validaciones de **Great Expectations**.")

df_logs = load_logs_from_postgres()

if df_logs.empty:
    st.info("Aún no hay registros en la tabla `execution_logs` de PostgreSQL. Ejecuta tus pipelines en Airflow para poblar la información.")
    st.stop()

# --- FILTROS EN SIDEBAR ---
st.sidebar.header("Filtros")
datasets_disponibles = sorted(list(df_logs["dataset_name"].unique()))
selected_dataset = st.sidebar.selectbox("Seleccionar Dataset", ["Todos"] + datasets_disponibles)

filtered_df = (
    df_logs if selected_dataset == "Todos"
    else df_logs[df_logs["dataset_name"] == selected_dataset]
)

if not filtered_df.empty:
    # Crear opciones legibles para el selectbox basadas en la base de datos
    opciones = [
        f"{row['dataset_name']} | Run: {row['run_id'][:16]}... | {row['created_at']}"
        for _, row in filtered_df.iterrows()
    ]
    seleccion = st.sidebar.selectbox("Seleccionar Corrida", opciones)
    
    # Obtener el registro seleccionado
    selected_idx = opciones.index(seleccion)
    selected_log = filtered_df.iloc[selected_idx]

    st.subheader(f"Ejecución: {selected_log['dataset_name']}")
    st.caption(
        f"**Run ID:** `{selected_log['run_id']}` | "
        f"**Fecha de Registro:** {selected_log['created_at']}"
    )
    st.caption(f"⏱️ **Duración del proceso:** {selected_log['duration_seconds']} s")

    # KPIs de Volumen y Limpieza
    st.markdown("### 📦 Flujo de Registros")
    col1, col2, col3, col4 = st.columns(4)
    col1.metric("Leídos (Bronze)", selected_log["records_read"])
    col2.metric("Duplicados Eliminados", selected_log["duplicates_removed"])
    col3.metric("Cuarentena", selected_log["records_quarantined"])
    col4.metric("Aprobados (Silver)", selected_log["records_validated"])

    st.markdown("---")

    # KPIs de Calidad
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

    # Extraer fallos detallados del contenido del log guardado en texto
    content = selected_log["log_content"]
    fallos = []
    if content and "DETALLE DE FALLOS" in content:
        try:
            fallos_section = content.split("DETALLE DE FALLOS")[1].split("=")[0]
            fallos = [
                line.strip()
                for line in fallos_section.split("\n")
                if line.strip().startswith("-")
            ]
        except Exception:
            pass

    if fallidas > 0 and fallos:
        st.error("⚠️ Fallos detectados en el esquema:")
        for fallo in fallos:
            st.warning(fallo)
    elif evaluadas > 0 and fallidas == 0:
        st.success("✅ Todas las reglas de Great Expectations fueron aprobadas exitosamente.")

    st.markdown("---")

    # Log completo almacenado en Postgres
    with st.expander("Ver Log Completo de la Ejecución"):
        st.code(selected_log["log_content"], language="text")