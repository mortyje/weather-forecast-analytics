SELECT
    event_id,
    observed_at,
    COUNT(*) AS duplicate_count
FROM {{ ref('int_meteostat__observation_hours') }}
GROUP BY
    event_id,
    observed_at
HAVING COUNT(*) > 1
