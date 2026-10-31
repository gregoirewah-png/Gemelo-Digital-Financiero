import os
import psycopg2
from groq import Groq

class AgenteFinancieroGroq:

    def __init__(self):

        # Inicialización opcional: no bloquear Streamlit si falta la API Key
        api_key = os.environ.get("GROQ_API_KEY")

        self.client = (
            Groq(api_key=api_key)
            if api_key
            else None
        )

        # Modelo vigente de Groq
        self.model = os.environ.get(
            "GROQ_MODEL",
            "openai/gpt-oss-120b"
        )

    def consultar_capa_gold(self, query_sql: str, params=None) -> str:
        """
        Consulta de solo lectura a PostgreSQL.
        La cuenta de BD debe tener permisos limitados a Gold.
        No pasar SQL arbitrario generado directamente por el usuario.
        """

        conn = None
        cursor = None

        try:

            conn = psycopg2.connect(
                dbname=os.environ.get("POSTGRES_DB", "gemelo_db"),
                user=os.environ.get("POSTGRES_USER", "admin"),
                password=os.environ.get("POSTGRES_PASSWORD", "admin"),
                host=os.environ.get("POSTGRES_HOST", "postgres"),
                port=os.environ.get("POSTGRES_PORT", "5432")
            )

            # Transacción de solo lectura
            conn.set_session(readonly=True)

            cursor = conn.cursor()

            # Evitar consultas demasiado largas
            cursor.execute(
                "SET LOCAL statement_timeout = '5000'"
            )

            cursor.execute(query_sql, params)

            if cursor.description is None:
                return "La consulta no devolvió resultados tabulares."

            resultados = cursor.fetchmany(100)

            return str(resultados)

        except Exception as e:

            if conn:
                conn.rollback()

            return (
                "Error consultando la capa Gold: "
                f"{str(e)}"
            )

        finally:

            if cursor:
                cursor.close()

            if conn:
                conn.close()

    def generar_respuesta(self, prompt_usuario: str) -> str:
        """
        Envía la consulta del usuario a Groq.
        """

        # Comprobar disponibilidad de IA
        if self.client is None:
            return (
                "El asistente financiero no está disponible. "
                "Falta configurar GROQ_API_KEY."
            )

        try:

            chat_completion = self.client.chat.completions.create(

                messages=[

                    {
                        "role": "system",

                        "content": (
                            "Eres el asistente inteligente del "
                            "Gemelo Digital Financiero Personal, "
                            "un prototipo académico de análisis financiero. "

                            "Tu función es ayudar a comprender las finanzas "
                            "personales, organizar gastos, analizar ingresos, "
                            "ahorros, deudas y comportamiento de crédito. "

                            "Puedes ayudar a evaluar la conveniencia de "
                            "compras y simular escenarios financieros. "

                            "No inventes cifras ni afirmes haber consultado "
                            "datos que no se te hayan proporcionado. "

                            "Cuando falte información, indícalo claramente. "
                            "Explica tus conclusiones de manera profesional, "
                            "clara y comprensible."
                        )
                    },

                    {
                        "role": "user",
                        "content": prompt_usuario
                    }

                ],

                model=self.model,
                temperature=0.3,
                max_tokens=1024

            )

            return (
                chat_completion.choices[0]
                .message.content
            )

        except Exception as e:

            return (
                "Error al comunicarse con Groq: "
                f"{str(e)}"
            )


# Instancia global: Streamlit ya no falla si falta la API Key
agente_financiero = AgenteFinancieroGroq()