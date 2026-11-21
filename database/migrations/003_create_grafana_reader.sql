\set ON_ERROR_STOP on
\getenv reader_password GRAFANA_READER_PASSWORD

BEGIN;

SELECT 'CREATE ROLE grafana_reader LOGIN'
WHERE NOT EXISTS (
    SELECT 1
    FROM pg_roles
    WHERE rolname = 'grafana_reader'
)
\gexec

ALTER ROLE grafana_reader PASSWORD :'reader_password';
ALTER ROLE grafana_reader SET default_transaction_read_only = on;

SELECT format(
    'GRANT CONNECT ON DATABASE %I TO grafana_reader',
    current_database()
)
\gexec

GRANT USAGE ON SCHEMA dm TO grafana_reader;
GRANT SELECT ON ALL TABLES IN SCHEMA dm TO grafana_reader;

-- Preserve access when dbt rebuilds analytics tables.
SELECT format(
    'ALTER DEFAULT PRIVILEGES FOR ROLE %I IN SCHEMA dm '
    'GRANT SELECT ON TABLES TO grafana_reader',
    tableowner
)
FROM pg_tables
WHERE schemaname = 'dm'
GROUP BY tableowner
\gexec

COMMIT;
