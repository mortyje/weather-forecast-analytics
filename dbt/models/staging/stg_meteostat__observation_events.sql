WITH source_events AS (
    SELECT
        topic,
        partition_id,
        offset_id,
        message_key,
        payload::JSONB AS event_data,
        ingested_at
    FROM {{ source('weather_raw', 'observation_events') }}
),

meteostat_events AS (
    SELECT *
    FROM source_events
    WHERE event_data ->> 'source' = 'meteostat'
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

    (event_data ->> 'schema_version')::INTEGER
        AS schema_version,
    event_data ->> 'source' AS source_id,
    event_data ->> 'source_name' AS source_name,
    event_data ->> 'city_id' AS city_id,

    event_data #>> '{location,id}' AS location_id,
    event_data #>> '{location,name}' AS location_name,
    (event_data #>> '{location,latitude}')::NUMERIC(9,6)
        AS location_latitude,
    (event_data #>> '{location,longitude}')::NUMERIC(9,6)
        AS location_longitude,
    event_data #>> '{location,timezone}' AS location_timezone,

    event_data #>> '{station,id}' AS station_id,
    event_data #>> '{station,name}' AS station_name,

    (event_data ->> 'collected_at_utc')::TIMESTAMPTZ
        AS collected_at,
    (event_data #>> '{observation_window,start_utc}')::TIMESTAMPTZ
        AS window_start,
    (event_data #>> '{observation_window,end_utc}')::TIMESTAMPTZ
        AS window_end,

    event_data -> 'observations' AS observations,
    ingested_at,
    (event_data #>> '{station,latitude}')::NUMERIC(9,6)
        AS station_latitude,
    (event_data #>> '{station,longitude}')::NUMERIC(9,6)
        AS station_longitude,
    (event_data #>> '{station,assigned_at_utc}')::TIMESTAMPTZ
        AS station_assigned_at

FROM meteostat_events
