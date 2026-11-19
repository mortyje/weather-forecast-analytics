SELECT
    event_id,
    city_id,
    location_id,
    location_name,
    location_timezone,
    location_latitude,
    location_longitude,
    requested_latitude,
    requested_longitude
FROM {{ ref('stg_open_meteo__forecast_events') }}
WHERE schema_version = 2
    AND (
        location_id IS DISTINCT FROM city_id
        OR location_name IS NULL
        OR location_timezone IS NULL
        OR location_latitude IS NULL
        OR location_longitude IS NULL
        OR source_name IS NULL
        OR location_latitude IS DISTINCT FROM requested_latitude
        OR location_longitude IS DISTINCT FROM requested_longitude
        OR location_latitude NOT BETWEEN -90 AND 90
        OR location_longitude NOT BETWEEN -180 AND 180
    )
