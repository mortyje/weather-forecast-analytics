SELECT
    archive_run_id,
    forecast_for,
    temperature_forecast_c,
    temperature_actual_c,
    temperature_error_c
FROM {{ ref('fact_archive_forecast_accuracy') }}
WHERE
    temperature_error_c IS DISTINCT FROM
        temperature_forecast_c - temperature_actual_c

    OR (
        station_selection_method = 'no_station'
        AND (
            has_observation
            OR station_key IS NOT NULL
        )
    )
