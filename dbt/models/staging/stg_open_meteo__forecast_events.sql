WITH source_events AS (
    SELECT
        topic,
        partition_id,
        offset_id,
        message_key,
        payload::JSONB AS event_data,
        ingested_at
    FROM {{ source('weather_raw', 'forecast_events') }}
),

open_meteo_events AS (
    SELECT *
    FROM source_events
    WHERE event_data ->> 'source' = 'open_meteo'
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
    message_key,

    event_data ->> 'source' AS source_id,
    event_data ->> 'city_id' AS city_id,

    (event_data ->> 'requested_at_utc')::TIMESTAMPTZ AS requested_at,
    (event_data ->> 'collected_at_utc')::TIMESTAMPTZ AS collected_at,

    (event_data #>> '{request,parameters,latitude}')::NUMERIC(9,6)
        AS requested_latitude,

    (event_data #>> '{request,parameters,longitude}')::NUMERIC(9,6)
        AS requested_longitude,

    (event_data #>> '{payload,latitude}')::NUMERIC(9,6)
        AS response_latitude,

    (event_data #>> '{payload,longitude}')::NUMERIC(9,6)
        AS response_longitude,

    event_data #> '{payload,hourly}' AS hourly_data,

    ingested_at

FROM open_meteo_events
