FROM apache/airflow:2.9.1-python3.11
USER root
RUN apt-get update && apt-get install -y --no-install-recommends openjdk-17-jre-headless ca-certificates curl \
    && rm -rf /var/lib/apt/lists/*
ENV JAVA_HOME=/usr/lib/jvm/java-17-openjdk-arm64
# El enlace funciona tanto en arm64 como en amd64.
RUN ln -s $(dirname $(dirname $(readlink -f /usr/bin/java))) /opt/java
ENV JAVA_HOME=/opt/java
USER airflow
COPY requirements.txt /tmp/requirements.txt
RUN pip install --no-cache-dir "apache-airflow==2.9.1" -r /tmp/requirements.txt && pip check
RUN python -c "import pathlib,pyspark,urllib.request; jars=pathlib.Path(pyspark.__file__).parent/'jars'; [(urllib.request.urlretrieve('https://repo.maven.apache.org/maven2/'+p, jars/p.rsplit('/',1)[-1])) for p in ['org/apache/hadoop/hadoop-aws/3.3.4/hadoop-aws-3.3.4.jar','com/amazonaws/aws-java-sdk-bundle/1.12.262/aws-java-sdk-bundle-1.12.262.jar','io/delta/delta-spark_2.12/3.2.0/delta-spark_2.12-3.2.0.jar','io/delta/delta-storage/3.2.0/delta-storage-3.2.0.jar']]"
ENV SPARK_JARS_BUNDLED=1 PYTHONPATH=/opt/airflow/project PYTHONUNBUFFERED=1
WORKDIR /opt/airflow/project
COPY --chown=airflow:root src ./src
COPY --chown=airflow:root dags ./dags
COPY --chown=airflow:root configs ./configs
COPY --chown=airflow:root knowledge ./knowledge
COPY --chown=airflow:root schemas ./schemas
COPY --chown=airflow:root sql ./sql
COPY --chown=airflow:root scripts ./scripts
COPY --chown=airflow:root tests ./tests
COPY --chown=airflow:root .streamlit ./.streamlit
COPY --chown=airflow:root data/raw ./data/raw
COPY --chown=airflow:root data/test ./data/test
COPY --chown=airflow:root data/demo ./data/demo
COPY --chown=airflow:root docs ./docs
COPY --chown=airflow:root ["README.md", "Documento de Arquitectura.md", ".env.example", "./"]
USER root
RUN mkdir -p logs models && chown airflow:root . logs models && chmod -R g+rwX logs models
USER airflow
ENV GEMELO_VERSION=2026.11.19-movil-rag
LABEL org.opencontainers.image.title="Gemelo Digital Financiero" \
      org.opencontainers.image.version="2026.11.19-movil-rag"
