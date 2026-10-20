import os
import glob
import re
from datetime import datetime
import psycopg2

def get_db_connection():
    return psycopg2.connect(
        dbname="gemelo_db",        # Ajusta el nombre de tu BD si es diferente
        user="admin",           # Tu usuario de postgres
        password="admin", # Tu contraseña de postgres
        host="postgres",    # El nombre del servicio en docker-compose
        port="5432"
    )

def parse_and_insert_logs():
    BASE_DIR = os.path.dirname(os.path.abspath(__file__))
    log_dir = os.path.join(BASE_DIR, "..", "logs") # Ajusta según la ruta de tu proyecto
    log_files = glob.glob(os.path.join(log_dir, "*.log"))
    
    if not log_files:
        print("No se encontraron archivos de log físicos.")
        return

    conn = get_db_connection()
    cursor = conn.cursor()

    for file_path in log_files:
        with open(file_path, "r", encoding="utf-8") as f:
            content = f.read()

        # Extraer métricas usando expresiones regulares básicas
        filename = os.path.basename(file_path)
        dataset_match = re.search(r"dataset(\d+)", filename, re.IGNORECASE)
        dataset_name = f"Dataset {dataset_match.group(1)}" if dataset_match else "Desconocido"

        def extract_val(pattern, text, default=0, cast_type=int):
            match = re.search(pattern, text)
            if match:
                try:
                    return cast_type(match.group(1))
                except ValueError:
                    return default
            return default

        run_id_match = re.search(r"Run ID:\s*(.+)", content) 
        mathrm_run_id = run_id_match.group(1).strip() if run_id_match else "manual_run"

        duracion = extract_val(r"Duración del proceso:\s*([\d\.]+)", content, 0.0, float)
        leidos = extract_val(r"Registros leídos.*?:\s*(\d+)", content)
        duplicados = extract_val(r"Duplicados eliminados:\s*(\d+)", content)
        cuarentena = extract_val(r"Registros aislados.*?:\s*(\d+)", content)
        validados = extract_val(r"Registros validados.*?:\s*(\d+)", content)
        evaluadas = extract_val(r"Reglas de calidad evaluadas:\s*(\d+)", content)
        fallidas = extract_val(r"Reglas fallidas:\s*(\d+)", content)

        # Insertar en PostgreSQL
        insert_query = """
            INSERT INTO execution_logs 
            (dataset_name, run_id, duration_seconds, records_read, duplicates_removed, 
             records_quarantined, records_validated, expectations_evaluated, expectations_failed, log_content)
            VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
        """
        cursor.execute(insert_query, (
            dataset_name, r_run_id if 'r_run_id' in locals() else "manual", duracion, 
            leidos, duplicados, cuarentena, validados, evaluadas, fallidas, content
        ))

    conn.commit()
    cursor.close()
    conn.close()
    print("¡Logs históricos migrados exitosamente a PostgreSQL!")

if __name__ == "__main__":
    parse_and_insert_logs()