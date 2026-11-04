import json
import os
import re
from pathlib import Path
from typing import Any, Optional

import psycopg2
from groq import Groq


PROJECT_ROOT = Path(__file__).resolve().parents[2]
DEFAULT_CATALOG_PATH = PROJECT_ROOT / "configs" / "data_catalog.json"


def load_data_catalog() -> tuple[dict[str, Any], Optional[str]]:
    """Carga el catálogo de metadatos. Devuelve un aviso si el archivo no existe o es inválido."""
    catalog_path = Path(os.getenv("DATA_CATALOG_PATH", str(DEFAULT_CATALOG_PATH)))

    try:
        with catalog_path.open("r", encoding="utf-8") as catalog_file:
            catalog = json.load(catalog_file)

        if not isinstance(catalog, dict) or not isinstance(catalog.get("datasets"), dict):
            raise ValueError("El catálogo debe ser un objeto JSON con una sección 'datasets'.")

        return catalog, None

    except FileNotFoundError:
        return {}, f"No se encontró el catálogo de datos en {catalog_path}."
    except (json.JSONDecodeError, OSError, ValueError) as exc:
        return {}, f"No se pudo cargar el catálogo de datos: {exc}"


class AgenteFinancieroGroq:
    def __init__(self) -> None:
        api_key = os.getenv("GROQ_API_KEY")
        self.client = Groq(api_key=api_key) if api_key else None
        self.model = os.getenv("GROQ_MODEL", "openai/gpt-oss-120b")

        self.catalog, self.catalog_warning = load_data_catalog()
        self.catalog_context = json.dumps(
            self.catalog,
            ensure_ascii=False,
            indent=2
        ) if self.catalog else "Catálogo de datos no disponible. No inventes metadatos."

    def estado(self) -> dict[str, Any]:
        """Permite al dashboard mostrar si IA y catálogo están disponibles."""
        return {
            "groq_configured": self.client is not None,
            "catalog_loaded": bool(self.catalog),
            "catalog_warning": self.catalog_warning,
            "model": self.model
        }

    def consultar_capa_gold(self, query_sql: str, params: Optional[tuple] = None) -> str:
        """Ejecuta una consulta SQL de solo lectura preparada por código confiable.

        No se debe pasar SQL generado directamente a partir del mensaje del usuario.
        La cuenta de PostgreSQL también debe tener permisos de solo lectura y acceso
        únicamente a las vistas/tablas Gold necesarias.
        """
        if not query_sql or not re.match(r"^\s*(SELECT|WITH)\b", query_sql, re.IGNORECASE):
            return "Consulta rechazada: solo se permiten consultas de lectura."

        # Rechazar múltiples sentencias. Se permite un punto y coma final.
        sql_without_trailing_semicolon = query_sql.strip().removesuffix(";")
        if ";" in sql_without_trailing_semicolon:
            return "Consulta rechazada: no se permiten múltiples sentencias SQL."

        conn = None
        cursor = None

        try:
            conn = psycopg2.connect(
                dbname=os.getenv("POSTGRES_DB", "gemelo_db"),
                user=os.getenv("POSTGRES_USER", "admin"),
                password=os.getenv("POSTGRES_PASSWORD", "admin"),
                host=os.getenv("POSTGRES_HOST", "postgres"),
                port=os.getenv("POSTGRES_PORT", "5432"),
                connect_timeout=5
            )
            conn.set_session(readonly=True, autocommit=False)
            cursor = conn.cursor()
            cursor.execute("SET LOCAL statement_timeout = '5000'")
            cursor.execute(query_sql, params)

            if cursor.description is None:
                return "La consulta no devolvió un conjunto de resultados."

            rows = cursor.fetchmany(100)
            return json.dumps(rows, ensure_ascii=False, default=str)

        except Exception as exc:
            if conn:
                conn.rollback()
            return f"Error consultando la capa Gold: {exc}"

        finally:
            if cursor:
                cursor.close()
            if conn:
                conn.close()

    def generar_respuesta(self, prompt_usuario: str) -> str:
        if not prompt_usuario or not prompt_usuario.strip():
            return "Escribe una pregunta para poder ayudarte."

        if self.client is None:
            return (
                "El asistente financiero no está disponible porque falta configurar "
                "GROQ_API_KEY en el entorno del contenedor Streamlit."
            )

        system_prompt = f"""
Eres el asistente del proyecto académico Gemelo Digital Financiero Personal.
Ayudas a interpretar metadatos, estructura, calidad y posibles análisis de datos
financieros personales. No afirmes ser un asesor oficial de BBVA ni una entidad
financiera.

CATÁLOGO DE DATOS DEL PROYECTO (fuente de verdad para preguntas sobre datasets):
{self.catalog_context}

REGLAS OBLIGATORIAS:
1. Usa el catálogo para responder preguntas sobre Dataset 1, Dataset 2 y Dataset 3.
2. Distingue filas originales de filas de las versiones sucias. Explica qué representa
   cada conteo y no confundas conteos históricos de perfilado con conteos actuales de
   Bronze, Silver o Gold.
3. Distingue columnas originales (source_name) de nombres estandarizados en Silver
   (silver_name).
4. No inventes columnas, valores, métricas, fechas, resultados de ejecución, tablas,
   vistas ni relaciones entre datasets.
5. Los indicadores marcados como propuestos no deben presentarse como ya calculados
   o disponibles en Gold.
6. Cuando un indicador requiera confirmar su unidad, escala, periodo o cobertura,
   dilo explícitamente.
7. Respeta las reglas contextuales de valores nulos, especialmente loan_type cuando
   has_loan = No.
8. Si preguntan por resultados actuales de Gold o por datos reales de una persona y
   el catálogo no los contiene, explica que necesitas consultar las tablas Gold ya
   implementadas; no simules esa consulta.
9. Responde en español, de forma clara, concreta y con suficiente contexto. Usa tablas
   cuando ayuden a comparar columnas, calidad o indicadores.
10. Distingue claramente hechos del catálogo, propuestas analíticas y limitaciones.
""".strip()

        try:
            completion = self.client.chat.completions.create(
                model=self.model,
                messages=[
                    {"role": "system", "content": system_prompt},
                    {"role": "user", "content": prompt_usuario.strip()}
                ],
                temperature=0.2,
                max_tokens=1400
            )

            answer = completion.choices[0].message.content
            return answer.strip() if answer else "No se recibió una respuesta del modelo."

        except Exception as exc:
            return f"Error al comunicarse con Groq: {exc}"

    def __call__(self, prompt_usuario: str) -> str:
        """Compatibilidad con código del dashboard que invoque el agente como función."""
        return self.generar_respuesta(prompt_usuario)


# Instancia global para importar desde Streamlit.
agente_financiero = AgenteFinancieroGroq()
