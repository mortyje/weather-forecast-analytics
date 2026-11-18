import argparse
import json
import sys
from datetime import UTC, datetime
from pathlib import Path

import httpx

from weather_ingestion.locations import load_locations
from weather_ingestion.open_meteo import (
    FORECAST_URL,
    OpenMeteoClient,
    build_forecast_params,
)


def main() -> None:
    parser = argparse.ArgumentParser(
        description="Collect weather forecasts from Open-Meteo"
    )

    parser.add_argument(
        "--locations",
        type=Path,
        default=Path("/app/config/locations.json"),
    )

    args = parser.parse_args()

    try:
        locations = load_locations(args.locations)
    except (OSError, ValueError) as exc:
        parser.exit(2, f"Invalid locations config: {exc}\n")

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
            except (httpx.HTTPError, ValueError) as exc:
                failed_locations += 1
                print(
                    f"Failed to collect forecast for {location.id}: {exc}",
                    file=sys.stderr,
                )
                continue

            collected_at = datetime.now(UTC).isoformat()

            message = {
                "source": "open_meteo",
                "city_id": location.id,
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

            print(json.dumps(message, ensure_ascii=False))

    if failed_locations:
        parser.exit(
            1,
            f"Forecast collection failed for {failed_locations} location(s)\n",
        )


if __name__ == "__main__":
    main()
