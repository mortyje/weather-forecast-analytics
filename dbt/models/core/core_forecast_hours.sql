{{ config(
    alias='forecast_hours',
    unique_key=['snapshot_id', 'forecast_for']
) }}

WITH forecast_hours AS (
    SELECT
        snapshots.snapshot_id,
        hours.forecast_for,
        hours.temperature_c,
        hours.humidity_pct,
        hours.precipitation_mm,
        hours.wind_speed_kmh
    FROM {{ ref('int_open_meteo__forecast_hours') }} AS hours
    INNER JOIN {{ ref('core_forecast_snapshots') }} AS snapshots
        ON hours.event_id = snapshots.snapshot_id
)

SELECT
    hours.*
FROM forecast_hours AS hours

{% if is_incremental() %}
WHERE NOT EXISTS (
    SELECT 1
    FROM {{ this }} AS existing
    WHERE existing.snapshot_id = hours.snapshot_id
        AND existing.forecast_for = hours.forecast_for
)
{% endif %}
