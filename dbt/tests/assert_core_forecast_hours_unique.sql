SELECT
    snapshot_id,
    forecast_for,
    COUNT(*) AS duplicate_count
FROM {{ ref('core_forecast_hours') }}
GROUP BY
    snapshot_id,
    forecast_for
HAVING COUNT(*) > 1
