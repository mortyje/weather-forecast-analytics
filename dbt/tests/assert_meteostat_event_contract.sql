SELECT
    event_id,
    city_id,
    location_id,
    station_id,
    schema_version
FROM {{ ref('stg_meteostat__observation_events') }}
WHERE
    location_id IS DISTINCT FROM city_id
    OR NULLIF(TRIM(station_id), '') IS NULL
    OR schema_version IS NULL
    OR collected_at IS NULL
    OR window_start IS NULL
    OR window_end IS NULL
    OR window_start > window_end
    OR CASE
        WHEN jsonb_typeof(observations) = 'array'
            THEN jsonb_array_length(observations) = 0
        ELSE TRUE
    END
