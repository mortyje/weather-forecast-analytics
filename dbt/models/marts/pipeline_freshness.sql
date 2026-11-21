{{ config(
    materialized='view',
    alias='pipeline_freshness'
) }}

WITH freshness_thresholds AS (
    SELECT *
    FROM (
        VALUES
            ('open_meteo', 6),
            ('meteostat', 36)
    ) AS thresholds(source_id, max_lag_hours)
),

locations AS (
    SELECT
        location_id,
        name AS location_name
    FROM {{ ref('core_locations') }}
),

source_events AS (
    SELECT
        location_id,
        source_id,
        collected_at,
        ingested_at
    FROM {{ ref('stg_open_meteo__forecast_events') }}

    UNION ALL

    SELECT
        location_id,
        source_id,
        collected_at,
        ingested_at
    FROM {{ ref('stg_meteostat__observation_events') }}
    WHERE ingestion_mode = 'scheduled'
),

latest_events AS (
    SELECT
        location_id,
        source_id,
        MAX(collected_at) AS last_collected_at,
        MAX(ingested_at) AS last_ingested_at,
        COUNT(*) AS event_count
    FROM source_events
    GROUP BY
        location_id,
        source_id
),

location_sources AS (
    SELECT
        locations.location_id,
        locations.location_name,
        thresholds.source_id,
        thresholds.max_lag_hours
    FROM locations
    CROSS JOIN freshness_thresholds AS thresholds
),

freshness AS (
    SELECT
        expected.location_id,
        expected.location_name,
        expected.source_id,
        expected.max_lag_hours,

        actual.last_collected_at,
        actual.last_ingested_at,
        COALESCE(actual.event_count, 0) AS event_count,

        ROUND(
            (
                EXTRACT(
                    EPOCH FROM (
                        CURRENT_TIMESTAMP - actual.last_collected_at
                    )
                ) / 3600
            )::NUMERIC,
            2
        ) AS age_hours

    FROM location_sources AS expected

    LEFT JOIN latest_events AS actual
        ON expected.location_id = actual.location_id
        AND expected.source_id = actual.source_id
)

SELECT
    location_id,
    location_name,
    source_id,

    last_collected_at,
    last_ingested_at,

    event_count,
    age_hours,
    max_lag_hours,

    CASE
        WHEN last_collected_at IS NULL
            THEN 'no_data'

        WHEN last_collected_at > CURRENT_TIMESTAMP
            + INTERVAL '10 minutes'
            THEN 'invalid_time'

        WHEN last_collected_at < CURRENT_TIMESTAMP
            - MAKE_INTERVAL(hours => max_lag_hours)
            THEN 'stale'

        ELSE 'fresh'
    END AS freshness_status

FROM freshness
