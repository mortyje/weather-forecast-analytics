SELECT
    snapshot_id,
    forecast_for,
    COUNT(*) AS duplicate_count
FROM {{ ref('fact_forecast_accuracy') }}
GROUP BY snapshot_id, forecast_for
HAVING COUNT(*) > 1
