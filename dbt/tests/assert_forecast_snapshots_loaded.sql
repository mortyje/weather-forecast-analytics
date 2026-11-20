SELECT
    staging.event_id,
    staging.city_id,
    staging.collected_at
FROM {{ ref('stg_open_meteo__forecast_events') }} AS staging
LEFT JOIN {{ ref('core_forecast_snapshots') }} AS snapshots
    ON staging.event_id = snapshots.snapshot_id
WHERE snapshots.snapshot_id IS NULL
