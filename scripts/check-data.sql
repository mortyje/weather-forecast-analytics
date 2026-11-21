\set ON_ERROR_STOP on

BEGIN READ ONLY;

DO $$
BEGIN
    -- Required analytical relations.
    IF to_regclass('dm.fact_forecast_accuracy') IS NULL
        OR to_regclass(
            'dm.fact_archive_forecast_accuracy'
        ) IS NULL
        OR to_regclass('dm.pipeline_freshness') IS NULL
    THEN
        RAISE EXCEPTION
            'Required Data Marts are missing';
    END IF;

    -- Unique actual observations.
    IF EXISTS (
        SELECT 1
        FROM core.weather_observations
        GROUP BY station_key, observed_at
        HAVING COUNT(*) > 1
    ) THEN
        RAISE EXCEPTION
            'Duplicate core weather observations';
    END IF;

    -- Unique archived model runs.
    IF EXISTS (
        SELECT 1
        FROM core.archive_forecast_runs
        GROUP BY
            location_id,
            model_id,
            model_run_at
        HAVING COUNT(*) > 1
    ) THEN
        RAISE EXCEPTION
            'Duplicate archive forecast runs';
    END IF;

    -- Unique live forecast facts.
    IF EXISTS (
        SELECT 1
        FROM dm.fact_forecast_accuracy
        GROUP BY snapshot_id, forecast_for
        HAVING COUNT(*) > 1
    ) THEN
        RAISE EXCEPTION
            'Duplicate live forecast facts';
    END IF;

    -- Unique archive forecast facts.
    IF EXISTS (
        SELECT 1
        FROM dm.fact_archive_forecast_accuracy
        GROUP BY archive_run_id, forecast_for
        HAVING COUNT(*) > 1
    ) THEN
        RAISE EXCEPTION
            'Duplicate archive forecast facts';
    END IF;

    -- Live error calculation.
    IF EXISTS (
        SELECT 1
        FROM dm.fact_forecast_accuracy
        WHERE temperature_error_c IS DISTINCT FROM
            temperature_forecast_c - temperature_actual_c
    ) THEN
        RAISE EXCEPTION
            'Invalid live temperature errors';
    END IF;

    -- Archive error calculation.
    IF EXISTS (
        SELECT 1
        FROM dm.fact_archive_forecast_accuracy
        WHERE temperature_error_c IS DISTINCT FROM
            temperature_forecast_c - temperature_actual_c
    ) THEN
        RAISE EXCEPTION
            'Invalid archive temperature errors';
    END IF;

    IF NOT (
        has_table_privilege('grafana_reader', 'dm.fact_forecast_accuracy', 'SELECT')
        AND has_table_privilege('grafana_reader', 'dm.fact_archive_forecast_accuracy', 'SELECT')
        AND has_table_privilege('grafana_reader', 'dm.pipeline_freshness', 'SELECT')
    ) THEN
        RAISE EXCEPTION 'Grafana reader is missing SELECT permissions';
    END IF;

    IF EXISTS (
        SELECT 1
        FROM pg_tables
        WHERE schemaname IN ('core', 'dm')
            AND has_table_privilege(
                'grafana_reader',
                format('%I.%I', schemaname, tablename),
                'INSERT, UPDATE, DELETE, TRUNCATE'
            )
    ) THEN
        RAISE EXCEPTION 'Grafana reader has unexpected write permissions';
    END IF;

    RAISE NOTICE 'Core, Data Marts and Grafana reader checks passed';
END
$$;

-- Diagnostic information.
SELECT
    'live_forecast_facts' AS dataset,
    COUNT(*) AS row_count
FROM dm.fact_forecast_accuracy

UNION ALL

SELECT
    'archive_forecast_facts',
    COUNT(*)
FROM dm.fact_archive_forecast_accuracy

UNION ALL

SELECT
    'weather_observations',
    COUNT(*)
FROM core.weather_observations

UNION ALL

SELECT
    'archive_model_runs',
    COUNT(*)
FROM core.archive_forecast_runs;

-- Current monitoring status.
SELECT
    source_id,
    freshness_status,
    COUNT(*) AS location_count
FROM dm.pipeline_freshness
GROUP BY source_id, freshness_status
ORDER BY source_id, freshness_status;

COMMIT;
