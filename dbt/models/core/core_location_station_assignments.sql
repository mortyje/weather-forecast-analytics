{{ config(
    alias='location_station_assignments',
    unique_key='assignment_id'
) }}

WITH assignment_events AS (
    SELECT
        location_id,
        MD5(CONCAT(source_id, ':', station_id)) AS station_key,
        station_assigned_at AS valid_from,
        collected_at
    FROM {{ ref('stg_meteostat__observation_events') }}
    WHERE station_assigned_at IS NOT NULL
        AND location_id IS NOT NULL
        AND source_id IS NOT NULL
        AND station_id IS NOT NULL
),

distinct_assignments AS (
    SELECT
        location_id,
        station_key,
        valid_from,
        MIN(collected_at) AS first_seen_at,
        MAX(collected_at) AS last_seen_at
    FROM assignment_events
    GROUP BY location_id, station_key, valid_from
),

assignment_history AS (
    SELECT
        *,
        LEAD(valid_from) OVER (
            PARTITION BY location_id
            ORDER BY valid_from, station_key
        ) AS valid_to
    FROM distinct_assignments
)

SELECT
    MD5(
        CONCAT(
            location_id,
            ':',
            station_key,
            ':',
            EXTRACT(EPOCH FROM valid_from)::TEXT
        )
    ) AS assignment_id,
    location_id,
    station_key,
    valid_from,
    valid_to,
    first_seen_at,
    last_seen_at
FROM assignment_history
