WITH raw_events AS (
    SELECT
        topic,
        partition_id,
        offset_id,
        payload::JSONB AS event_data,
        ingested_at
    FROM {{ source('weather_raw', 'forecast_events') }}
    WHERE payload::JSONB ->> 'forecast_kind' = 'archived_run'
)

SELECT
    MD5(
        CONCAT_WS(
            ':',
            topic,
            partition_id::TEXT,
            offset_id::TEXT
        )
    ) AS event_id,

    topic,
    partition_id,
    offset_id,

    (event_data ->> 'schema_version')::INTEGER
        AS schema_version,

    event_data ->> 'source' AS source_id,
    event_data ->> 'forecast_kind' AS forecast_kind,
    event_data ->> 'model_id' AS model_id,

    (event_data ->> 'model_run_at_utc')::TIMESTAMPTZ
        AS model_run_at,

    (event_data ->> 'collected_at_utc')::TIMESTAMPTZ
        AS collected_at,

    event_data ->> 'city_id' AS location_id,

    event_data -> 'forecast' AS forecast_json,

    ingested_at

FROM raw_events
