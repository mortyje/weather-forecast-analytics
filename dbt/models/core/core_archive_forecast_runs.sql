{{ config(
    alias='archive_forecast_runs',
    unique_key='archive_run_id'
) }}

WITH archive_events AS (
    SELECT
        event_id,
        location_id,
        model_id,
        model_run_at,
        collected_at,
        ingested_at,

        ROW_NUMBER() OVER (
            PARTITION BY
                location_id,
                model_id,
                model_run_at
            ORDER BY
                collected_at DESC NULLS LAST,
                ingested_at DESC,
                event_id DESC
        ) AS event_version

    FROM {{ ref('stg_open_meteo__archive_events') }}

    WHERE location_id IS NOT NULL
        AND model_id IS NOT NULL
        AND model_run_at IS NOT NULL
),

latest_runs AS (
    SELECT *
    FROM archive_events
    WHERE event_version = 1
)

SELECT
    MD5(
        CONCAT_WS(
            ':',
            location_id,
            model_id,
            EXTRACT(EPOCH FROM model_run_at)::BIGINT::TEXT
        )
    ) AS archive_run_id,

    location_id,
    model_id,
    model_run_at,

    event_id AS source_event_id,
    collected_at AS source_collected_at,
    ingested_at AS source_ingested_at

FROM latest_runs
