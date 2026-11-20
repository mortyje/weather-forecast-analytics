
CREATE SCHEMA IF NOT EXISTS control;

CREATE TABLE IF NOT EXISTS control.station_assignments (
    location_id TEXT PRIMARY KEY,

    location_latitude NUMERIC(9, 6) NOT NULL,
    location_longitude NUMERIC(9, 6) NOT NULL,

    station_id TEXT NOT NULL,
    station_name TEXT NOT NULL,
    station_latitude NUMERIC(9, 6) NOT NULL,
    station_longitude NUMERIC(9, 6) NOT NULL,
    distance_km NUMERIC(8, 2) NOT NULL,

    temperature_coverage NUMERIC(5, 4) NOT NULL,
    humidity_coverage NUMERIC(5, 4) NOT NULL,
    wind_coverage NUMERIC(5, 4) NOT NULL,
    precipitation_coverage NUMERIC(5, 4) NOT NULL,

    evaluation_period_start TIMESTAMPTZ NOT NULL,
    evaluation_period_end TIMESTAMPTZ NOT NULL,

    selected_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),

    CONSTRAINT valid_station_distance
        CHECK (distance_km >= 0),

    CONSTRAINT valid_temperature_coverage
        CHECK (temperature_coverage BETWEEN 0 AND 1),

    CONSTRAINT valid_humidity_coverage
        CHECK (humidity_coverage BETWEEN 0 AND 1),

    CONSTRAINT valid_wind_coverage
        CHECK (wind_coverage BETWEEN 0 AND 1),

    CONSTRAINT valid_precipitation_coverage
        CHECK (precipitation_coverage BETWEEN 0 AND 1),

    CONSTRAINT valid_evaluation_period
        CHECK (evaluation_period_start <= evaluation_period_end)
);
