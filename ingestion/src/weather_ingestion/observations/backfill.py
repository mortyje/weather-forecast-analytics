import argparse
import json
import logging
import math
import os
import sys
import time
from collections.abc import Iterator
from datetime import date, datetime, timedelta
from pathlib import Path

import psycopg
from psycopg.rows import dict_row

from weather_ingestion.observations.collection import build_message, serialize_observations
from weather_ingestion.observations.meteostat_client import (
    configure_meteostat,
    fetch_hourly_observations,
)
from weather_ingestion.observations.repository import assignment_matches_location, get_assignment
from weather_ingestion.shared.kafka_publisher import KafkaPublisher
from weather_ingestion.shared.locations import Location, load_locations


def iter_time_windows(
    start: date, end: date, chunk_days: int = 7
) -> Iterator[tuple[datetime, datetime]]:
    """Yield adjacent half-open UTC windows for an inclusive range of dates."""
    if start > end:
        raise ValueError("Start date must be on or before end date")
    if end == date.max:
        raise ValueError("End date must allow an exclusive upper boundary")
    if not 1 <= chunk_days <= 31:
        raise ValueError("Chunk size must be between 1 and 31 days")

    current = datetime.combine(start, datetime.min.time())
    stop = datetime.combine(end + timedelta(days=1), datetime.min.time())
    while current < stop:
        following = current + timedelta(days=min(chunk_days, (stop - current).days))
        yield current, following
        current = following


def select_locations(locations: list[Location], city: str | None) -> list[Location]:
    if city is None:
        return locations
    for location in locations:
        if location.id == city:
            return [location]
    raise ValueError(f"Unknown city: {city}")


class MeteostatFailureHandler(logging.Handler):
    failure: str | None = None

    def emit(self, record: logging.LogRecord) -> None:
        if record.exc_info or record.levelno >= logging.ERROR:
            self.failure = record.getMessage()


def fetch_window(station_id: str, start: datetime, end: datetime):
    # Meteostat 2.2.3 logs failed downloads and can return an empty series.
    logger = logging.getLogger("meteostat")
    handler = MeteostatFailureHandler(logging.WARNING)
    logger.addHandler(handler)
    try:
        data = fetch_hourly_observations(station_id, start, end)
    finally:
        logger.removeHandler(handler)
    if handler.failure is not None:
        raise RuntimeError(handler.failure)
    return data


def main() -> None:
    parser = argparse.ArgumentParser(description="Backfill hourly observations from Meteostat")
    parser.add_argument("--locations", type=Path, required=True)
    parser.add_argument("--start", type=date.fromisoformat, required=True, help="First UTC day")
    parser.add_argument("--end", type=date.fromisoformat, required=True, help="Last UTC day")
    parser.add_argument("--chunk-days", type=int, default=7)
    parser.add_argument("--city", help="Location ID from the locations file")
    parser.add_argument("--output", choices=("jsonl", "kafka"), default="jsonl")
    parser.add_argument("--dry-run", action="store_true", help="Print the planned windows")
    parser.add_argument("--sleep-seconds", type=float, default=1.0)
    args = parser.parse_args()

    if not math.isfinite(args.sleep_seconds) or args.sleep_seconds < 0:
        parser.error("--sleep-seconds must be finite and non-negative")
    try:
        windows = list(iter_time_windows(args.start, args.end, args.chunk_days))
        locations = select_locations(load_locations(args.locations), args.city)
    except (OSError, ValueError) as exc:
        parser.error(str(exc))

    if args.dry_run:
        for location in locations:
            for start, stop in windows:
                print(f"{location.id}: [{start.isoformat()}Z, {stop.isoformat()}Z)")
        print(f"Planned windows: {len(locations) * len(windows)}", file=sys.stderr)
        return

    configure_meteostat()
    publisher = (
        KafkaPublisher(
            os.getenv("KAFKA_BOOTSTRAP_SERVERS", "kafka:9092"),
            os.getenv("KAFKA_OBSERVATION_TOPIC", "weather.observations.raw"),
        )
        if args.output == "kafka"
        else None
    )

    exported = empty = failed = 0
    query_sent = False
    try:
        connection = psycopg.connect(autocommit=True, row_factory=dict_row)
    except psycopg.Error as exc:
        raise SystemExit(f"Could not read station assignments: {exc}") from exc

    with connection:
        for location in locations:
            try:
                assignment = get_assignment(connection, location.id)
                if assignment is None:
                    raise ValueError("No station assignment found")
                if not assignment_matches_location(assignment, location):
                    raise ValueError("Station assignment is outdated")
            except (psycopg.Error, ValueError) as exc:
                failed += len(windows)
                print(f"{location.id}: {exc}", file=sys.stderr)
                continue

            for start, stop in windows:
                window = f"{location.id} [{start.date()}, {stop.date()})"
                try:
                    if query_sent and args.sleep_seconds:
                        time.sleep(args.sleep_seconds)
                    query_sent = True
                    end = stop - timedelta(seconds=1)
                    data = fetch_window(assignment["station_id"], start, end)
                    if data is None or data.empty:
                        empty += 1
                        print(f"{window}: empty", file=sys.stderr)
                        continue

                    observations = serialize_observations(data)
                    message = build_message(
                        location, assignment, observations, start, end, ingestion_mode="backfill"
                    )
                    if publisher is None:
                        print(json.dumps(message, ensure_ascii=False, allow_nan=False))
                    else:
                        publisher.publish(message)
                    exported += 1
                    print(f"{window}: {len(observations)} observation hours", file=sys.stderr)
                except Exception as exc:
                    failed += 1
                    print(f"{window}: {type(exc).__name__}: {exc}", file=sys.stderr)

    delivery_failed = False
    if publisher is not None:
        try:
            publisher.flush()
        except RuntimeError as exc:
            delivery_failed = True
            print(str(exc), file=sys.stderr)

    output_status = "queued" if delivery_failed else "exported"
    print(f"Backfill: {output_status}={exported}, empty={empty}, failed={failed}", file=sys.stderr)
    if failed or delivery_failed:
        raise SystemExit(1)


if __name__ == "__main__":
    main()
