{{ config(
    alias='weather_sources',
    unique_key='source_id'
) }}

WITH source_events AS (
    SELECT
        event_id,
        source_id,
        source_name,
        collected_at,
        ingested_at
    FROM {{ ref('stg_open_meteo__forecast_events') }}

    UNION ALL

    SELECT
        event_id,
        source_id,
        source_name,
        collected_at,
        ingested_at
    FROM {{ ref('stg_meteostat__observation_events') }}
),

valid_sources AS (
    SELECT *
    FROM source_events
    WHERE source_id IS NOT NULL
        AND source_name IS NOT NULL
),

ranked_sources AS (
    SELECT
        *,
        ROW_NUMBER() OVER (
            PARTITION BY source_id
            ORDER BY collected_at DESC, ingested_at DESC, event_id DESC
        ) AS source_version
    FROM valid_sources
)

SELECT
    source_id,
    source_name AS name,
    collected_at AS last_seen_at
FROM ranked_sources
WHERE source_version = 1
