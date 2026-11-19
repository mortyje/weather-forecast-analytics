WITH forecast_events AS (
    SELECT
        event_id,
        city_id,
        collected_at,
        hourly_data
    FROM {{ ref('stg_open_meteo__forecast_events') }}
),

forecast_hours AS (
    SELECT
        events.event_id,
        events.city_id,
        events.collected_at,
        events.hourly_data,
        hours.forecast_time,
        hours.hour_position
    FROM forecast_events AS events
    CROSS JOIN LATERAL jsonb_array_elements_text(
        CASE
            WHEN jsonb_typeof(events.hourly_data -> 'time') = 'array'
                THEN events.hourly_data -> 'time'
            ELSE '[]'::JSONB
        END
    ) WITH ORDINALITY AS hours(forecast_time, hour_position)
)

SELECT
    event_id,
    city_id,
    collected_at,

    (forecast_time::TIMESTAMP AT TIME ZONE 'UTC')
        AS forecast_for,

    (
        hourly_data -> 'temperature_2m'
        ->> ((hour_position - 1)::INTEGER)
    )::DOUBLE PRECISION AS temperature_c,

    (
        hourly_data -> 'relative_humidity_2m'
        ->> ((hour_position - 1)::INTEGER)
    )::DOUBLE PRECISION AS humidity_pct,

    (
        hourly_data -> 'precipitation'
        ->> ((hour_position - 1)::INTEGER)
    )::DOUBLE PRECISION AS precipitation_mm,

    (
        hourly_data -> 'wind_speed_10m'
        ->> ((hour_position - 1)::INTEGER)
    )::DOUBLE PRECISION AS wind_speed_kmh,

    hour_position

FROM forecast_hours
