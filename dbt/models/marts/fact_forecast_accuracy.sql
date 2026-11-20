WITH forecasts AS (
    SELECT
        snapshots.snapshot_id,
        snapshots.location_id,
        snapshots.collected_at,
        hours.forecast_for,
        (
            EXTRACT(
                EPOCH FROM (
                    hours.forecast_for
                    - DATE_TRUNC('hour', snapshots.collected_at, 'UTC')
                )
            ) / 3600
        )::INTEGER AS horizon_hours,
        hours.temperature_c AS temperature_forecast_c,
        hours.humidity_pct AS humidity_forecast_pct,
        hours.precipitation_mm AS precipitation_forecast_mm,
        hours.wind_speed_kmh AS wind_forecast_kmh
    FROM {{ ref('core_forecast_hours') }} AS hours
    INNER JOIN {{ ref('core_forecast_snapshots') }} AS snapshots
        ON hours.snapshot_id = snapshots.snapshot_id
    WHERE hours.forecast_for >= DATE_TRUNC('hour', snapshots.collected_at, 'UTC')
),

forecasts_with_stations AS (
    SELECT
        forecasts.*,
        assignments.assignment_id,
        assignments.station_key
    FROM forecasts
    LEFT JOIN {{ ref('core_location_station_assignments') }} AS assignments
        ON forecasts.location_id = assignments.location_id
        AND forecasts.forecast_for >= assignments.valid_from
        AND (assignments.valid_to IS NULL OR forecasts.forecast_for < assignments.valid_to)
),

matched_observations AS (
    SELECT
        forecasts.*,
        observations.observed_at,
        observations.temperature_c AS temperature_actual_c,
        observations.humidity_pct AS humidity_actual_pct,
        observations.precipitation_mm AS precipitation_actual_mm,
        observations.wind_speed_kmh AS wind_actual_kmh
    FROM forecasts_with_stations AS forecasts
    LEFT JOIN {{ ref('core_weather_observations') }} AS observations
        ON forecasts.station_key = observations.station_key
        AND forecasts.forecast_for = observations.observed_at
)

SELECT
    snapshot_id,
    location_id,
    station_key,
    assignment_id,
    collected_at,
    forecast_for,
    horizon_hours,
    (observed_at IS NOT NULL) AS has_observation,
    temperature_forecast_c,
    temperature_actual_c,
    temperature_forecast_c - temperature_actual_c AS temperature_error_c,
    humidity_forecast_pct,
    humidity_actual_pct,
    humidity_forecast_pct - humidity_actual_pct AS humidity_error_pct,
    precipitation_forecast_mm,
    precipitation_actual_mm,
    precipitation_forecast_mm - precipitation_actual_mm AS precipitation_error_mm,
    wind_forecast_kmh,
    wind_actual_kmh,
    wind_forecast_kmh - wind_actual_kmh AS wind_error_kmh
FROM matched_observations
