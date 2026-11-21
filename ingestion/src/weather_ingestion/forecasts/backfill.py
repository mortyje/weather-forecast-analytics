import argparse
import json
import math
import os
import sys
from collections.abc import Iterator
from datetime import UTC, date, datetime, time, timedelta
from pathlib import Path
from time import sleep

import httpx
from confluent_kafka import KafkaException

from weather_ingestion.forecasts.open_meteo import (
    SOURCE_ID,
    SOURCE_NAME,
    build_forecast_params,
    validate_forecast_response,
)
from weather_ingestion.shared.kafka_publisher import KafkaPublisher
from weather_ingestion.shared.locations import Location, load_locations

API_URL = "https://single-runs-api.open-meteo.com/v1/forecast"
MODEL_ID = "ecmwf_ifs"
MODEL_HOURS = (0, 6, 12, 18)


def iter_model_runs(start: date, end: date) -> Iterator[datetime]:
    """Return the UTC model runs for an inclusive date range."""
    if start > end:
        raise ValueError("Start date must be on or before end date")
    return (
        datetime.combine(start + timedelta(days=offset), time(hour, tzinfo=UTC))
        for offset in range((end - start).days + 1)
        for hour in MODEL_HOURS
    )


def select_locations(locations: list[Location], city: str | None) -> list[Location]:
    if city is None:
        return locations
    for location in locations:
        if location.id == city:
            return [location]
    raise ValueError(f"Unknown city: {city}")


def fetch_model_run(client: httpx.Client, location: Location, model_run_at: datetime) -> dict:
    run_time = model_run_at.astimezone(UTC).replace(tzinfo=None)
    params = build_forecast_params(location.latitude, location.longitude)
    params.update(models=MODEL_ID, run=run_time.isoformat(timespec="minutes"), timezone="GMT")
    response = client.get(API_URL, params=params)
    response.raise_for_status()
    return validate_forecast_response(response.json())


def build_archive_message(location: Location, model_run_at: datetime, forecast: dict) -> dict:
    return {
        "schema_version": 2,
        "source": SOURCE_ID,
        "source_name": SOURCE_NAME,
        "forecast_kind": "archived_run",
        "city_id": location.id,
        "location": {
            "id": location.id,
            "name": location.name,
            "latitude": location.latitude,
            "longitude": location.longitude,
            "timezone": location.timezone,
        },
        "model_id": MODEL_ID,
        "model_run_at_utc": model_run_at.astimezone(UTC).isoformat(),
        "collected_at_utc": datetime.now(UTC).isoformat(),
        "forecast": forecast,
    }


def main() -> None:
    parser = argparse.ArgumentParser(description="Backfill individual Open-Meteo ECMWF IFS runs")
    parser.add_argument("--start", type=date.fromisoformat, required=True, help="First UTC day")
    parser.add_argument("--end", type=date.fromisoformat, required=True, help="Last UTC day")
    parser.add_argument("--locations", type=Path, default=Path("/app/config/locations.json"))
    parser.add_argument("--city", help="Location ID from the locations file")
    parser.add_argument("--output", choices=("jsonl", "kafka"), default="jsonl")
    parser.add_argument("--dry-run", action="store_true", help="Print the planned runs")
    parser.add_argument("--sleep-seconds", type=float, default=1.0)
    args = parser.parse_args()

    if not math.isfinite(args.sleep_seconds) or args.sleep_seconds < 0:
        parser.error("--sleep-seconds must be finite and non-negative")
    try:
        runs = iter_model_runs(args.start, args.end)
        locations = select_locations(load_locations(args.locations), args.city)
    except (OSError, ValueError) as exc:
        parser.error(str(exc))

    if args.dry_run:
        requests = 0
        for model_run_at in runs:
            for location in locations:
                print(f"{location.id}: {MODEL_ID} {model_run_at.isoformat()}")
                requests += 1
        print(f"Planned requests: {requests}", file=sys.stderr)
        return

    publisher = (
        KafkaPublisher(
            os.getenv("KAFKA_BOOTSTRAP_SERVERS", "kafka:9092"),
            os.getenv("KAFKA_FORECAST_TOPIC", "weather.forecasts.raw"),
        )
        if args.output == "kafka"
        else None
    )

    exported = failed = 0
    with httpx.Client(
        timeout=30.0,
        headers={"User-Agent": "weather-forecast-analytics/0.1"},
    ) as client:
        for model_run_at in runs:
            for location in locations:
                if (exported or failed) and args.sleep_seconds:
                    sleep(args.sleep_seconds)
                try:
                    forecast = fetch_model_run(client, location, model_run_at)
                    message = build_archive_message(location, model_run_at, forecast)
                    if publisher is None:
                        print(json.dumps(message, ensure_ascii=False, allow_nan=False))
                    else:
                        publisher.publish(message)
                    exported += 1
                except (httpx.HTTPError, ValueError, KafkaException, BufferError) as exc:
                    failed += 1
                    print(f"{location.id} {model_run_at.isoformat()}: {exc}", file=sys.stderr)

    delivery_failed = False
    if publisher is not None:
        try:
            publisher.flush()
        except RuntimeError as exc:
            delivery_failed = True
            print(str(exc), file=sys.stderr)

    output_status = "queued" if delivery_failed else "exported"
    print(f"Forecast backfill: {output_status}={exported}, failed={failed}", file=sys.stderr)
    if failed or delivery_failed:
        raise SystemExit(1)


if __name__ == "__main__":
    main()
