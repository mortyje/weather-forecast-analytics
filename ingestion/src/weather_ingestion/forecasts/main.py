import argparse
import json
import os
import sys
from datetime import UTC, datetime
from pathlib import Path

import httpx
from confluent_kafka import KafkaException

from weather_ingestion.forecasts.open_meteo import (
    FORECAST_URL,
    SOURCE_ID,
    SOURCE_NAME,
    OpenMeteoClient,
    build_forecast_params,
)
from weather_ingestion.shared.kafka_publisher import KafkaPublisher
from weather_ingestion.shared.locations import load_locations


def main() -> None:
    parser = argparse.ArgumentParser(description="Collect weather forecasts from Open-Meteo")

    parser.add_argument(
        "--locations",
        type=Path,
        default=Path("/app/config/locations.json"),
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
        default=os.getenv("KAFKA_FORECAST_TOPIC", "weather.forecasts.raw"),
    )

    args = parser.parse_args()

    try:
        locations = load_locations(args.locations)
    except (OSError, ValueError) as exc:
        parser.exit(2, f"Invalid locations config: {exc}\n")

    publisher = (
        KafkaPublisher(args.bootstrap_servers, args.topic) if args.output == "kafka" else None
    )

    failed_locations = 0

    with httpx.Client(
        timeout=15.0,
        headers={"User-Agent": "weather-forecast-analytics/0.1"},
    ) as http_client:
        client = OpenMeteoClient(http_client)

        for location in locations:
            requested_at = datetime.now(UTC).isoformat()

            try:
                forecast = client.get_forecast(
                    latitude=location.latitude,
                    longitude=location.longitude,
                )

                collected_at = datetime.now(UTC).isoformat()

                message = {
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
                    "requested_at_utc": requested_at,
                    "collected_at_utc": collected_at,
                    "request": {
                        "url": FORECAST_URL,
                        "parameters": build_forecast_params(
                            location.latitude,
                            location.longitude,
                        ),
                    },
                    "payload": forecast,
                }

                if publisher is None:
                    print(json.dumps(message, ensure_ascii=False, allow_nan=False))
                else:
                    publisher.publish(message)

            except (
                httpx.HTTPError,
                ValueError,
                KafkaException,
                BufferError,
            ) as exc:
                failed_locations += 1
                print(
                    f"Failed to process {location.id}: {exc}",
                    file=sys.stderr,
                )

    if publisher is not None:
        try:
            publisher.flush()
        except RuntimeError as exc:
            failed_locations += 1
            print(str(exc), file=sys.stderr)

    if failed_locations:
        parser.exit(1, "Forecast collection or delivery failed\n")


if __name__ == "__main__":
    main()
