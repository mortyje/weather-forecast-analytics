WITH archive_events AS (
    SELECT
        event_id,
        location_id,
        model_id,
        model_run_at,
        collected_at,
        ingested_at,
        forecast_json
    FROM {{ ref('stg_open_meteo__archive_events') }}
),

hourly_forecasts AS (
    SELECT
        events.event_id,
        events.location_id,
        events.model_id,
        events.model_run_at,
        events.collected_at,
        events.ingested_at,

        (
            hours.value::TIMESTAMP
            AT TIME ZONE 'UTC'
        ) AS forecast_for,

        temperatures.value::NUMERIC
            AS temperature_c,

        humidities.value::NUMERIC
            AS humidity_pct,

        precipitations.value::NUMERIC
            AS precipitation_mm,

        winds.value::NUMERIC
            AS wind_speed_kmh

    FROM archive_events AS events

    CROSS JOIN LATERAL jsonb_array_elements_text(
        events.forecast_json #> '{hourly,time}'
    ) WITH ORDINALITY AS hours(value, position)

    LEFT JOIN LATERAL jsonb_array_elements_text(
        events.forecast_json #> '{hourly,temperature_2m}'
    ) WITH ORDINALITY AS temperatures(value, position)
        ON temperatures.position = hours.position

    LEFT JOIN LATERAL jsonb_array_elements_text(
        events.forecast_json #> '{hourly,relative_humidity_2m}'
    ) WITH ORDINALITY AS humidities(value, position)
        ON humidities.position = hours.position

    LEFT JOIN LATERAL jsonb_array_elements_text(
        events.forecast_json #> '{hourly,precipitation}'
    ) WITH ORDINALITY AS precipitations(value, position)
        ON precipitations.position = hours.position

    LEFT JOIN LATERAL jsonb_array_elements_text(
        events.forecast_json #> '{hourly,wind_speed_10m}'
    ) WITH ORDINALITY AS winds(value, position)
        ON winds.position = hours.position
)

SELECT
    *,
    (
        EXTRACT(
            EPOCH FROM (forecast_for - model_run_at)
        ) / 3600
    )::INTEGER AS model_horizon_hours
FROM hourly_forecasts
WHERE forecast_for >= model_run_at
