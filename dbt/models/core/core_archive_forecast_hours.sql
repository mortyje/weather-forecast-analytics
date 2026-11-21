{{ config(
    materialized='table',
    alias='archive_forecast_hours'
) }}

SELECT
    runs.archive_run_id,

    hours.forecast_for,
    hours.model_horizon_hours,

    hours.temperature_c,
    hours.humidity_pct,
    hours.precipitation_mm,
    hours.wind_speed_kmh

FROM {{ ref('core_archive_forecast_runs') }} AS runs

INNER JOIN {{ ref('int_open_meteo__archive_hours') }} AS hours
    ON runs.source_event_id = hours.event_id

WHERE hours.model_horizon_hours BETWEEN 0 AND 168
