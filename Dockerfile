FROM apache/airflow:2.9.1

USER root

RUN apt-get update \
    && apt-get install -y --no-install-recommends \
    default-jre-headless \
    && apt-get autoremove -yqq --purge \
    && apt-get clean \
    && rm -rf /var/lib/apt/lists/*

ENV JAVA_HOME=/usr/lib/jvm/default-java

USER airflow
RUN pip install --no-cache-dir streamlit pandas "great_expectations<1.0.0" psycopg2-binary
COPY requirements.txt /requirements.txt
RUN pip install --no-cache-dir -r /requirements.txt