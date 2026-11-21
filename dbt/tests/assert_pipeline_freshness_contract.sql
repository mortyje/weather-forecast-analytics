WITH invalid_records AS (
    SELECT
        location_id,
        source_id
    FROM {{ ref('pipeline_freshness') }}
    WHERE max_lag_hours <= 0
        OR event_count < 0
        OR (
            freshness_status = 'no_data'
            AND last_collected_at IS NOT NULL
        )
),

duplicate_records AS (
    SELECT
        location_id,
        source_id
    FROM {{ ref('pipeline_freshness') }}
    GROUP BY
        location_id,
        source_id
    HAVING COUNT(*) > 1
)

SELECT *
FROM invalid_records

UNION ALL

SELECT *
FROM duplicate_records
