SELECT
    station_key,
    observed_at,
    COUNT(*) AS duplicate_count
FROM {{ ref('core_weather_observations') }}
GROUP BY station_key, observed_at
HAVING COUNT(*) > 1
