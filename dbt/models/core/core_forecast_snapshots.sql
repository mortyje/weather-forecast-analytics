{{ config(
    alias='forecast_snapshots',
    unique_key='snapshot_id'
) }}

WITH source_snapshots AS (
    SELECT
        event_id AS snapshot_id,
        city_id AS location_id,
        source_id,
        requested_at,
        collected_at,
        requested_latitude,
        requested_longitude,
        response_latitude,
        response_longitude,
        topic AS raw_topic,
        partition_id AS raw_partition,
        offset_id AS raw_offset
    FROM {{ ref('stg_open_meteo__forecast_events') }}
)

SELECT
    snapshots.*
FROM source_snapshots AS snapshots

{% if is_incremental() %}
WHERE NOT EXISTS (
    SELECT 1
    FROM {{ this }} AS existing
    WHERE existing.snapshot_id = snapshots.snapshot_id
)
{% endif %}
