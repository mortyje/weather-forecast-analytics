#!/bin/sh
set -eu

psql -X -v ON_ERROR_STOP=1 \
    -v airflow_password="$AIRFLOW_DB_PASSWORD" <<'SQL'

SELECT format(
    'CREATE ROLE airflow_user LOGIN PASSWORD %L',
    :'airflow_password'
)
WHERE NOT EXISTS (
    SELECT 1
    FROM pg_roles
    WHERE rolname = 'airflow_user'
)
\gexec

SELECT format(
    'ALTER ROLE airflow_user WITH PASSWORD %L',
    :'airflow_password'
)
\gexec

SELECT 'CREATE DATABASE airflow_meta OWNER airflow_user'
WHERE NOT EXISTS (
    SELECT 1
    FROM pg_database
    WHERE datname = 'airflow_meta'
)
\gexec

SQL
