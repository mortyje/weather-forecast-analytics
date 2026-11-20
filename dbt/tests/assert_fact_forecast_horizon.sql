SELECT
    snapshot_id,
    forecast_for,
    collected_at,
    horizon_hours
FROM {{ ref('fact_forecast_accuracy') }}
WHERE horizon_hours IS NULL
    OR horizon_hours NOT BETWEEN 0 AND 168
