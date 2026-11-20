WITH ranked_observations AS (
    SELECT
        MD5(CONCAT(source_id, ':', station_id)) AS station_key,
        observed_at,
        event_id,
        ROW_NUMBER() OVER (
            PARTITION BY source_id, station_id, observed_at
            ORDER BY collected_at DESC NULLS LAST, ingested_at DESC, event_id DESC
        ) AS observation_version
    FROM {{ ref('int_meteostat__observation_hours') }}
    WHERE source_id IS NOT NULL
        AND station_id IS NOT NULL
        AND observed_at IS NOT NULL
),

latest_observations AS (
    SELECT
        station_key,
        observed_at,
        event_id
    FROM ranked_observations
    WHERE observation_version = 1
)

SELECT
    latest.station_key,
    latest.observed_at,
    latest.event_id AS expected_event_id,
    actual.source_event_id AS actual_event_id
FROM latest_observations AS latest
LEFT JOIN {{ ref('core_weather_observations') }} AS actual
    ON latest.station_key = actual.station_key
    AND latest.observed_at = actual.observed_at
WHERE actual.source_event_id IS DISTINCT FROM latest.event_id
