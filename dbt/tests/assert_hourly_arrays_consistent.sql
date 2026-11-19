WITH forecast_events AS (
    SELECT
        event_id,
        hourly_data,
        CASE
            WHEN jsonb_typeof(hourly_data -> 'time') = 'array'
                THEN jsonb_array_length(hourly_data -> 'time')
        END AS expected_hours
    FROM {{ ref('stg_open_meteo__forecast_events') }}
),

hourly_fields AS (
    SELECT
        events.event_id,
        fields.field_name,
        events.expected_hours,

        jsonb_typeof(
            events.hourly_data -> fields.field_name
        ) AS field_type,

        CASE
            WHEN jsonb_typeof(
                events.hourly_data -> fields.field_name
            ) = 'array'
                THEN jsonb_array_length(
                    events.hourly_data -> fields.field_name
                )
        END AS actual_hours

    FROM forecast_events AS events

    CROSS JOIN (
        VALUES
            ('time'),
            ('temperature_2m'),
            ('relative_humidity_2m'),
            ('precipitation'),
            ('wind_speed_10m')
    ) AS fields(field_name)
)

SELECT
    event_id,
    field_name,
    expected_hours,
    actual_hours
FROM hourly_fields
WHERE
    field_type IS DISTINCT FROM 'array'
    OR expected_hours IS NULL
    OR expected_hours = 0
    OR actual_hours IS DISTINCT FROM expected_hours
