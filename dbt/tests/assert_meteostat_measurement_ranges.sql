SELECT
    event_id,
    station_id,
    observed_at,
    humidity_pct,
    precipitation_mm,
    wind_speed_kmh
FROM {{ ref('int_meteostat__observation_hours') }}
WHERE
    humidity_pct NOT BETWEEN 0 AND 100
    OR precipitation_mm < 0
    OR wind_speed_kmh < 0
