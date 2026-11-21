WITH forecasts AS (
    SELECT
        runs.archive_run_id,
        runs.location_id,
        runs.model_id,
        runs.model_run_at,

        hours.forecast_for,
        hours.model_horizon_hours,

        hours.temperature_c AS temperature_forecast_c,
        hours.humidity_pct AS humidity_forecast_pct,
        hours.precipitation_mm AS precipitation_forecast_mm,
        hours.wind_speed_kmh AS wind_forecast_kmh

    FROM {{ ref('core_archive_forecast_runs') }} AS runs

    INNER JOIN {{ ref('core_archive_forecast_hours') }} AS hours
        ON runs.archive_run_id = hours.archive_run_id

    WHERE hours.model_horizon_hours BETWEEN 6 AND 168
),

forecasts_with_stations AS (
    SELECT
        forecasts.*,

        assignments.assignment_id,
        assignments.station_key,

        CASE
            WHEN assignments.assignment_id IS NULL
                THEN 'no_station'

            WHEN assignments.matches_target_time
                THEN 'historical_assignment'

            ELSE 'retrospective_fallback'
        END AS station_selection_method

    FROM forecasts

    LEFT JOIN LATERAL (
        SELECT
            assignment.assignment_id,
            assignment.station_key,

            (
                forecasts.forecast_for >= assignment.valid_from
                AND (
                    assignment.valid_to IS NULL
                    OR forecasts.forecast_for < assignment.valid_to
                )
            ) AS matches_target_time

        FROM {{ ref('core_location_station_assignments') }}
            AS assignment

        WHERE assignment.location_id = forecasts.location_id

        ORDER BY
            matches_target_time DESC,
            assignment.valid_from DESC,
            assignment.assignment_id DESC

        LIMIT 1
    ) AS assignments ON TRUE
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

    LEFT JOIN {{ ref('core_weather_observations') }}
        AS observations

        ON forecasts.station_key = observations.station_key
        AND forecasts.forecast_for = observations.observed_at
)

SELECT
    archive_run_id,
    location_id,
    model_id,
    model_run_at,
    forecast_for,
    model_horizon_hours,

    assignment_id,
    station_key,
    station_selection_method,

    (observed_at IS NOT NULL) AS has_observation,

    temperature_forecast_c,
    temperature_actual_c,

    temperature_forecast_c - temperature_actual_c
        AS temperature_error_c,

    humidity_forecast_pct,
    humidity_actual_pct,

    humidity_forecast_pct - humidity_actual_pct
        AS humidity_error_pct,

    precipitation_forecast_mm,
    precipitation_actual_mm,

    precipitation_forecast_mm - precipitation_actual_mm
        AS precipitation_error_mm,

    wind_forecast_kmh,
    wind_actual_kmh,

    wind_forecast_kmh - wind_actual_kmh
        AS wind_error_kmh

FROM matched_observations
