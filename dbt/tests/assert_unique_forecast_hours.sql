SELECT
    event_id,
    forecast_for,
    COUNT(*) AS duplicate_count
FROM {{ ref('int_open_meteo__forecast_hours') }}
GROUP BY
    event_id,
    forecast_for
HAVING COUNT(*) > 1
