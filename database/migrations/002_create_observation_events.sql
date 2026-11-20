CREATE SCHEMA IF NOT EXISTS raw;

CREATE TABLE IF NOT EXISTS raw.observation_events (
    topic TEXT NOT NULL,
    partition_id INTEGER NOT NULL,
    offset_id BIGINT NOT NULL,

    message_key TEXT,
    payload TEXT,
    ingested_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),

    CONSTRAINT pk_observation_events
        PRIMARY KEY (topic, partition_id, offset_id)
);
