import argparse
import json
import os
import sys
from datetime import UTC, datetime
from pathlib import Path

import pandas as pd
import psycopg
from psycopg.rows import dict_row

from weather_ingestion.observations.meteostat_client import (
    configure_meteostat,
    fetch_hourly_observations,
    get_observation_period,
)
from weather_ingestion.observations.repository import (
    assignment_matches_location,
    get_assignment,
)
from weather_ingestion.shared.kafka_publisher import KafkaPublisher
from weather_ingestion.shared.locations import load_locations

SOURCE_ID = "meteostat"
SOURCE_NAME = "Meteostat"


def numeric_or_none(value) -> float | None:
    """Convert missing measurements to JSON-compatible null."""
    if pd.isna(value):
        return None

    return float(value)


def serialize_observations(data: pd.DataFrame) -> list[dict]:
    """Convert Meteostat rows into hourly observation records."""
    observations = []

    for timestamp, row in data.iterrows():
        observed_at = pd.Timestamp(timestamp)

        if observed_at.tzinfo is None:
            observed_at = observed_at.tz_localize("UTC")
        else:
            observed_at = observed_at.tz_convert("UTC")

        observations.append(
            {
                "observed_at_utc": observed_at.isoformat(),
                "temperature_c": numeric_or_none(row.get("temp")),
                "humidity_pct": numeric_or_none(row.get("rhum")),
                "precipitation_mm": numeric_or_none(row.get("prcp")),
                "wind_speed_kmh": numeric_or_none(row.get("wspd")),
            }
        )

    return observations


def build_message(location, assignment, observations, start, end):
    return {
        "schema_version": 2,
        "source": SOURCE_ID,
        "source_name": SOURCE_NAME,
        "city_id": location.id,
        "location": {
            "id": location.id,
            "name": location.name,
            "latitude": location.latitude,
            "longitude": location.longitude,
            "timezone": location.timezone,
        },
        "station": {
            "id": assignment["station_id"],
            "name": assignment["station_name"],
            "latitude": float(assignment["station_latitude"]),
            "longitude": float(assignment["station_longitude"]),
            "assigned_at_utc": assignment["selected_at"].astimezone(UTC).isoformat(),
        },
        "collected_at_utc": datetime.now(UTC).isoformat(),
        "observation_window": {
            "start_utc": start.replace(tzinfo=UTC).isoformat(),
            "end_utc": end.replace(tzinfo=UTC).isoformat(),
        },
        "observations": observations,
    }


def main() -> None:
    parser = argparse.ArgumentParser(description="Collect hourly observations from Meteostat")

    parser.add_argument(
        "--locations",
        type=Path,
        required=True,
    )
    parser.add_argument(
        "--output",
        choices=("jsonl", "kafka"),
        default="jsonl",
    )
    parser.add_argument(
        "--bootstrap-servers",
        default=os.getenv("KAFKA_BOOTSTRAP_SERVERS", "kafka:9092"),
    )
    parser.add_argument(
        "--topic",
        default=os.getenv(
            "KAFKA_OBSERVATION_TOPIC",
            "weather.observations.raw",
        ),
    )

    args = parser.parse_args()
    locations = load_locations(args.locations)

    configure_meteostat()
    start, end = get_observation_period()

    publisher = (
        KafkaPublisher(args.bootstrap_servers, args.topic) if args.output == "kafka" else None
    )

    failed_locations = 0

    with psycopg.connect(row_factory=dict_row) as connection:
        for location in locations:
            try:
                assignment = get_assignment(
                    connection,
                    location.id,
                )

                if assignment is None:
                    raise ValueError("No station assignment found")

                if not assignment_matches_location(
                    assignment,
                    location,
                ):
                    raise ValueError("Station assignment is outdated")

                data = fetch_hourly_observations(
                    assignment["station_id"],
                    start,
                    end,
                )

                if data is None or data.empty:
                    raise ValueError("No hourly observations available")

                observations = serialize_observations(data)

                message = build_message(
                    location,
                    assignment,
                    observations,
                    start,
                    end,
                )

                if publisher is None:
                    print(
                        json.dumps(
                            message,
                            ensure_ascii=False,
                            allow_nan=False,
                        )
                    )
                else:
                    publisher.publish(message)

                print(
                    f"{location.id}: collected "
                    f"{len(observations)} hours "
                    f"from {assignment['station_id']}",
                    file=sys.stderr,
                )

            except Exception as exc:
                failed_locations += 1
                print(
                    f"{location.id}: {type(exc).__name__}: {exc}",
                    file=sys.stderr,
                )

    if publisher is not None:
        try:
            publisher.flush()
        except RuntimeError as exc:
            failed_locations += 1
            print(str(exc), file=sys.stderr)

    if failed_locations:
        raise SystemExit(f"Collection failed for {failed_locations} operation(s)")


if __name__ == "__main__":
    main()
