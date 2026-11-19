FROM apache/airflow:3.3.2-python3.12

COPY requirements.txt /tmp/requirements.txt

RUN pip install --no-cache-dir \
    "apache-airflow==3.3.2" \
    -r /tmp/requirements.txt

USER root

RUN mkdir -p /opt/airflow/auth \
    && chown airflow:0 /opt/airflow/auth \
    && chmod 770 /opt/airflow/auth

USER airflow