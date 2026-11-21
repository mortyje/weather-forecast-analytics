WITH expected AS (
    SELECT
        event_id,
        COUNT(*) AS expected_hours
    FROM {{ ref('int_open_meteo__archive_hours') }}
    GROUP BY event_id
),

actual AS (
    SELECT
        runs.source_event_id AS event_id,
        COUNT(hours.forecast_for) AS actual_hours
    FROM {{ ref('core_archive_forecast_runs') }} AS runs
    LEFT JOIN {{ ref('core_archive_forecast_hours') }} AS hours
        ON runs.archive_run_id = hours.archive_run_id
    GROUP BY runs.source_event_id
)

SELECT
    actual.event_id,
    COALESCE(expected.expected_hours, 0) AS expected_hours,
    actual.actual_hours
FROM actual
LEFT JOIN expected
    ON actual.event_id = expected.event_id
WHERE actual.actual_hours
    IS DISTINCT FROM COALESCE(expected.expected_hours, 0)
