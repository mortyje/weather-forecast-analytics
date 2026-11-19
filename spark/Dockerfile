FROM apache/spark:4.2.1-java21-python3

USER root

RUN apt-get update \
    && apt-get install -y --no-install-recommends python3-venv \
    && rm -rf /var/lib/apt/lists/*

RUN python3 -m venv --system-site-packages /opt/spark/venv \
    && /opt/spark/venv/bin/pip install --no-cache-dir \
       "psycopg[binary]==3.3.9"

RUN mkdir -p /opt/spark/checkpoints /opt/spark/.ivy2 \
    && chown -R spark:spark \
       /opt/spark/checkpoints /opt/spark/.ivy2

ENV PYSPARK_PYTHON=/opt/spark/venv/bin/python \
    PYSPARK_DRIVER_PYTHON=/opt/spark/venv/bin/python \
    TZ=UTC

COPY --chown=spark:spark jobs /opt/spark/jobs/

USER spark
