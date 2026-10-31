FROM apache/airflow:2.9.1

USER root

# Instalamos dependencias del sistema y Java (OpenJDK) requerido por PySpark
RUN apt-get update && apt-get install -y \
    build-essential \
    libpq-dev \
    gcc \
    default-jdk \
    && rm -rf /var/lib/apt/lists/*

# Definimos la ruta correcta de Java para Debian/Ubuntu dentro de la imagen de Airflow
ENV JAVA_HOME=/usr/lib/jvm/default-java

USER airflow

# Instalación limpia de requerimientos y herramientas de compilación
RUN pip install --no-cache-dir --upgrade pip setuptools wheel
RUN pip install --no-cache-dir --prefer-binary streamlit pandas "great_expectations<1.0.0" psycopg2-binary pyspark

COPY --chown=airflow:root requirements.txt /requirements.txt
RUN pip install --no-cache-dir --prefer-binary -r /requirements.txt