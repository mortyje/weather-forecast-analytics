CREATE SCHEMA IF NOT EXISTS raw;

CREATE TABLE IF NOT EXISTS raw.forecast_events (
    topic               TEXT NOT NULL,
    partition_id        INTEGER NOT NULL,
    offset_id           BIGINT NOT NULL,
    message_key         TEXT,
    payload             TEXT NOT NULL,
    kafka_timestamp_ms  BIGINT,
    ingested_at         TIMESTAMPTZ NOT NULL DEFAULT NOW(),

    CONSTRAINT forecast_events_pk
        PRIMARY KEY (topic, partition_id, offset_id)
);
