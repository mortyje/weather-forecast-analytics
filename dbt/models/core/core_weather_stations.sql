{{ config(
    alias='weather_stations',
    unique_key='station_key'
) }}

WITH station_events AS (
    SELECT
        event_id,
        MD5(CONCAT(source_id, ':', station_id)) AS station_key,
        source_id,
        station_id,
        station_name,
        station_latitude,
        station_longitude,
        collected_at,
        ingested_at
    FROM {{ ref('stg_meteostat__observation_events') }}
    WHERE source_id IS NOT NULL
        AND station_id IS NOT NULL
),

ranked_stations AS (
    SELECT
        *,
        ROW_NUMBER() OVER (
            PARTITION BY station_key
            ORDER BY
                (station_latitude IS NOT NULL AND station_longitude IS NOT NULL) DESC,
                collected_at DESC,
                ingested_at DESC,
                event_id DESC
        ) AS station_version,
        MAX(collected_at) OVER (PARTITION BY station_key) AS last_seen_at
    FROM station_events
)

SELECT
    station_key,
    source_id,
    station_id,
    station_name AS name,
    station_latitude AS latitude,
    station_longitude AS longitude,
    last_seen_at
FROM ranked_stations
WHERE station_version = 1
