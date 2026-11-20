SELECT
    station_key,
    source_id,
    station_id,
    name,
    latitude,
    longitude
FROM {{ ref('core_weather_stations') }}
