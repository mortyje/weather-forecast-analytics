{{ config(
    alias='locations',
    unique_key='location_id'
) }}

WITH location_events AS (
    SELECT
        event_id,
        location_id,
        location_name,
        location_latitude,
        location_longitude,
        location_timezone,
        collected_at,
        ingested_at
    FROM {{ ref('stg_open_meteo__forecast_events') }}
    WHERE schema_version = 2
        AND location_id = city_id
        AND location_name IS NOT NULL
        AND location_latitude IS NOT NULL
        AND location_longitude IS NOT NULL
        AND location_timezone IS NOT NULL
),

latest_locations AS (
    SELECT
        *,
        ROW_NUMBER() OVER (
            PARTITION BY location_id
            ORDER BY collected_at DESC, ingested_at DESC, event_id DESC
        ) AS location_version
    FROM location_events
)

SELECT
    location_id,
    location_name AS name,
    location_latitude AS latitude,
    location_longitude AS longitude,
    location_timezone AS timezone,
    collected_at AS last_seen_at
FROM latest_locations
WHERE location_version = 1
