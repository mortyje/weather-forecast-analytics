SELECT
    archive_run_id,
    forecast_for,
    COUNT(*) AS duplicate_count
FROM {{ ref('fact_archive_forecast_accuracy') }}
GROUP BY
    archive_run_id,
    forecast_for
HAVING COUNT(*) > 1
