{{ config(
    alias='weather_observations',
    unique_key=['station_key', 'observed_at']
) }}

WITH source_observations AS (
    SELECT
        MD5(CONCAT(source_id, ':', station_id)) AS station_key,
        observed_at,
        temperature_c,
        humidity_pct,
        precipitation_mm,
        wind_speed_kmh,
        event_id,
        collected_at,
        ingested_at
    FROM {{ ref('int_meteostat__observation_hours') }}
    WHERE source_id IS NOT NULL
        AND station_id IS NOT NULL
        AND observed_at IS NOT NULL
),

ranked_observations AS (
    SELECT
        *,
        ROW_NUMBER() OVER (
            PARTITION BY station_key, observed_at
            ORDER BY collected_at DESC NULLS LAST, ingested_at DESC, event_id DESC
        ) AS observation_version
    FROM source_observations
),

latest_observations AS (
    SELECT
        station_key,
        observed_at,
        temperature_c,
        humidity_pct,
        precipitation_mm,
        wind_speed_kmh,
        event_id AS source_event_id,
        collected_at AS source_collected_at,
        ingested_at AS source_ingested_at
    FROM ranked_observations
    WHERE observation_version = 1
)

SELECT latest.*
FROM latest_observations AS latest
{% if is_incremental() %}
WHERE NOT EXISTS (
    SELECT 1
    FROM {{ this }} AS existing
    WHERE existing.station_key = latest.station_key
        AND existing.observed_at = latest.observed_at
        AND (
            existing.source_collected_at IS NOT NULL,
            COALESCE(existing.source_collected_at, '-infinity'::TIMESTAMPTZ),
            existing.source_ingested_at,
            existing.source_event_id
        ) >= (
            latest.source_collected_at IS NOT NULL,
            COALESCE(latest.source_collected_at, '-infinity'::TIMESTAMPTZ),
            latest.source_ingested_at,
            latest.source_event_id
        )
)
{% endif %}
