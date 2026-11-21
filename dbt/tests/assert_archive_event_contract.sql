WITH events AS (
    SELECT
        event_id,
        schema_version,
        source_id,
        forecast_kind,
        model_id,
        model_run_at,
        collected_at,
        forecast_json,

        forecast_json #> '{hourly,time}' AS hours,
        forecast_json #> '{hourly,temperature_2m}'
            AS temperatures,
        forecast_json #> '{hourly,relative_humidity_2m}'
            AS humidities,
        forecast_json #> '{hourly,precipitation}'
            AS precipitations,
        forecast_json #> '{hourly,wind_speed_10m}'
            AS winds

    FROM {{ ref('stg_open_meteo__archive_events') }}
)

SELECT event_id
FROM events
WHERE schema_version IS DISTINCT FROM 2
    OR source_id IS DISTINCT FROM 'open_meteo'
    OR forecast_kind IS DISTINCT FROM 'archived_run'
    OR model_id IS DISTINCT FROM 'ecmwf_ifs'
    OR model_run_at IS NULL
    OR collected_at IS NULL
    OR collected_at < model_run_at

    OR jsonb_typeof(hours) IS DISTINCT FROM 'array'
    OR jsonb_typeof(temperatures) IS DISTINCT FROM 'array'
    OR jsonb_typeof(humidities) IS DISTINCT FROM 'array'
    OR jsonb_typeof(precipitations) IS DISTINCT FROM 'array'
    OR jsonb_typeof(winds) IS DISTINCT FROM 'array'

    OR CASE
        WHEN jsonb_typeof(hours) = 'array'
            AND jsonb_typeof(temperatures) = 'array'
            AND jsonb_typeof(humidities) = 'array'
            AND jsonb_typeof(precipitations) = 'array'
            AND jsonb_typeof(winds) = 'array'
        THEN
            jsonb_array_length(hours) = 0
            OR jsonb_array_length(hours)
                <> jsonb_array_length(temperatures)
            OR jsonb_array_length(hours)
                <> jsonb_array_length(humidities)
            OR jsonb_array_length(hours)
                <> jsonb_array_length(precipitations)
            OR jsonb_array_length(hours)
                <> jsonb_array_length(winds)
        ELSE TRUE
    END
