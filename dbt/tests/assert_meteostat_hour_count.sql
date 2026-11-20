WITH expected_counts AS (
    SELECT
        event_id,
        jsonb_array_length(observations) AS expected_hours
    FROM {{ ref('stg_meteostat__observation_events') }}
    WHERE jsonb_typeof(observations) = 'array'
),

actual_counts AS (
    SELECT
        event_id,
        COUNT(*) AS actual_hours
    FROM {{ ref('int_meteostat__observation_hours') }}
    GROUP BY event_id
)

SELECT
    expected.event_id,
    expected.expected_hours,
    COALESCE(actual.actual_hours, 0) AS actual_hours
FROM expected_counts AS expected
LEFT JOIN actual_counts AS actual
    ON expected.event_id = actual.event_id
WHERE expected.expected_hours
    IS DISTINCT FROM COALESCE(actual.actual_hours, 0)
