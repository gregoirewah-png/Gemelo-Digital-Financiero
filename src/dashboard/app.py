import streamlit as st
import os
import glob
import re

st.set_page_config(
    page_title="Dashboard de Calidad | Airflow Medallion",
    page_icon="📊",
    layout="wide"
)

@st.cache_data
def load_airflow_logs(log_directory):
    # Búsqueda recursiva de logs de Airflow (attempt=*.log)
    log_files = glob.glob(
        os.path.join(log_directory, "**", "attempt=*.log"),
        recursive=True
    )
    # Ordenar por fecha de modificación (los más recientes primero)
    log_files.sort(key=os.path.getmtime, reverse=True)

    parsed_data = []

    patterns = {
        "Fecha": r"Fecha de ejecución:\s+(.*)",
        "Duracion_seg": r"Duración del proceso:\s+([0-9.]+)",
        "Leidos": r"Registros leídos \(Bronze\):\s+(\d+)",
        "Duplicados": r"Duplicados eliminados:\s+(\d+)",
        "Cuarentena": r"Registros aislados \(Cuarentena\):\s+(\d+)",
        "Validados": r"Registros validados listos para Silver:\s+(\d+)",
        "Reglas_Evaluadas": r"Reglas de calidad evaluadas:\s+(\d+)",
        "Reglas_Fallidas": r"Reglas fallidas:\s+(\d+)"
    }

    for filepath in log_files:
        try:
            with open(filepath, "r", encoding="utf-8") as f:
                content = f.read()
        except Exception:
            continue

        # Filtrar únicamente tareas que ejecutaron el reporte de calidad
        if "REPORTE DE CALIDAD TRAS INGESTA" not in content:
            continue

        # Extraer metadatos de la estructura de carpetas de Airflow
        dag_match = re.search(r"dag_id=([^/\\]+)", filepath)
        run_match = re.search(r"run_id=([^/\\]+)", filepath)
        task_match = re.search(r"task_id=([^/\\]+)", filepath)
        attempt_match = re.search(r"attempt=(\d+)\.log", filepath)

        dag_id = dag_match.group(1) if dag_match else "Desconocido"
        run_id = run_match.group(1) if run_match else "Desconocido"
        task_id = task_match.group(1) if task_match else "Desconocido"
        attempt = attempt_match.group(1) if attempt_match else "1"

        # Detectar a qué dataset corresponde la tarea
        dataset_match = re.search(r"dataset(\d+)", task_id, re.IGNORECASE)
        dataset_name = f"Dataset {dataset_match.group(1)}" if dataset_match else task_id

        metrics = {
            "Ruta_Archivo": filepath,
            "DAG": dag_id,
            "Run_ID": run_id,
            "Task_ID": task_id,
            "Intento": attempt,
            "Dataset": dataset_name
        }

        for key, pattern in patterns.items():
            match = re.search(pattern, content)
            metrics[key] = match.group(1) if match else "0"

        # Extraer fallos si Great Expectations reportó anomalías
        fallos = []
        if "DETALLE DE FALLOS EN SILVER:" in content:
            fallos_section = content.split("DETALLE DE FALLOS EN SILVER:\n")[1].split("=")[0]
            fallos = [
                line.strip()
                for line in fallos_section.split("\n")
                if line.strip().startswith("-")
            ]

        metrics["Detalle_Fallos"] = fallos
        metrics["Contenido_Raw"] = content
        parsed_data.append(metrics)

    return parsed_data


# --- INTERFAZ ---

st.title("📊 Monitor de Calidad de Datos (Airflow Logs)")
st.markdown("Auditoría de ejecuciones Medallion y validaciones de **Great Expectations**.")

BASE_DIR = os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
LOG_DIR = os.path.join(BASE_DIR, "logs")

if not os.path.exists(LOG_DIR):
    st.error(f"No se encontró la carpeta de logs: {LOG_DIR}")
    st.stop()

logs_data = load_airflow_logs(LOG_DIR)

if not logs_data:
    st.info("No se encontraron tareas con reportes de calidad en `logs/`. Ejecuta una tarea de transformación en Airflow.")
    st.stop()

# Filtros
st.sidebar.header("Filtros")
datasets_disponibles = sorted(list(set(d["Dataset"] for d in logs_data)))
selected_dataset = st.sidebar.selectbox("Seleccionar Dataset", ["Todos"] + datasets_disponibles)

filtered_logs = (
    logs_data if selected_dataset == "Todos"
    else [d for d in logs_data if d["Dataset"] == selected_dataset]
)

if filtered_logs:
    opciones = [
        f"{d['Dataset']} | {d['Fecha']} | Intento {d['Intento']}"
        for d in filtered_logs
    ]
    seleccion = st.sidebar.selectbox("Seleccionar Corrida", opciones)
    selected_log = filtered_logs[opciones.index(seleccion)]

    st.subheader(f"Ejecución: {selected_log['Dataset']}")
    st.caption(
        f"**DAG:** `{selected_log['DAG']}` | "
        f"**Tarea:** `{selected_log['Task_ID']}` | "
        f"**Run ID:** `{selected_log['Run_ID']}` | "
        f"**Intento:** {selected_log['Intento']}"
    )
    st.caption(f"📅 **Fecha:** {selected_log['Fecha']} | ⏱️ **Duración:** {selected_log['Duracion_seg']} s")

    # KPIs de Volumen y Limpieza
    st.markdown("### 📦 Flujo de Registros")
    col1, col2, col3, col4 = st.columns(4)
    col1.metric("Leídos (Bronze)", selected_log["Leidos"])
    col2.metric("Duplicados Eliminados", selected_log["Duplicados"])
    col3.metric("Cuarentena", selected_log["Cuarentena"])
    col4.metric("Aprobados (Silver)", selected_log["Validados"])

    st.markdown("---")

    # KPIs de Calidad
    st.markdown("### 🛡️ Reglas de Calidad (Great Expectations)")
    evaluadas = int(selected_log["Reglas_Evaluadas"])
    fallidas = int(selected_log["Reglas_Fallidas"])
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

    if fallidas > 0 and selected_log["Detalle_Fallos"]:
        st.error("⚠️ Fallos detectados en el esquema:")
        for fallo in selected_log["Detalle_Fallos"]:
            st.warning(fallo)
    elif evaluadas > 0 and fallidas == 0:
        st.success("✅ Todas las reglas de Great Expectations fueron aprobadas exitosamente.")

    st.markdown("---")

    # Log completo de Airflow
    with st.expander("Ver Log Completo de la Tarea en Airflow"):
        st.code(selected_log["Contenido_Raw"], language="text")