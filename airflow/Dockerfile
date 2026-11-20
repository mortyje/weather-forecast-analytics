FROM apache/airflow:3.3.2-python3.12

COPY airflow/requirements.txt /tmp/requirements.txt

COPY --chown=airflow:0 ingestion/pyproject.toml /tmp/weather-ingestion/pyproject.toml
COPY --chown=airflow:0 ingestion/src/ /tmp/weather-ingestion/src/

RUN pip install --no-cache-dir \
    "apache-airflow==3.3.2" \
    -r /tmp/requirements.txt \
    /tmp/weather-ingestion

USER root

RUN mkdir -p /opt/airflow/auth \
    && chown airflow:0 /opt/airflow/auth \
    && chmod 770 /opt/airflow/auth

USER airflow