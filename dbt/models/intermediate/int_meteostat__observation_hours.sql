WITH observation_events AS (
    SELECT
        event_id,
        source_id,
        location_id,
        station_id,
        collected_at,
        observations,
        ingested_at
    FROM {{ ref('stg_meteostat__observation_events') }}
)

SELECT
    events.event_id,
    events.source_id,
    events.location_id,
    events.station_id,
    events.collected_at,
    events.ingested_at,
    observation.observed_at_utc::TIMESTAMPTZ AS observed_at,
    observation.temperature_c,
    observation.humidity_pct,
    observation.precipitation_mm,
    observation.wind_speed_kmh

FROM observation_events AS events
CROSS JOIN LATERAL jsonb_to_recordset(
    CASE
        WHEN jsonb_typeof(events.observations) = 'array'
            THEN events.observations
        ELSE '[]'::JSONB
    END
) AS observation (
    observed_at_utc TEXT,
    temperature_c NUMERIC,
    humidity_pct NUMERIC,
    precipitation_mm NUMERIC,
    wind_speed_kmh NUMERIC
)
