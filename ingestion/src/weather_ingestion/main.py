import argparse
import json
import math
import sys
from datetime import UTC, datetime

import httpx

from weather_ingestion.open_meteo import (
    FORECAST_URL,
    OpenMeteoClient,
    build_forecast_params,
)


def main() -> None:
    parser = argparse.ArgumentParser(description="Collect weather forecasts from Open-Meteo")

    parser.add_argument("--city-id", required=True)
    parser.add_argument("--latitude", type=float, required=True)
    parser.add_argument("--longitude", type=float, required=True)

    args = parser.parse_args()

    if not math.isfinite(args.latitude) or not -90 <= args.latitude <= 90:
        parser.error("Latitude must be between -90 and 90")

    if not math.isfinite(args.longitude) or not -180 <= args.longitude <= 180:
        parser.error("Longitude must be between -180 and 180")

    requested_at = datetime.now(UTC).isoformat()

    try:
        with httpx.Client(
            timeout=15.0,
            headers={"User-Agent": "weather-forecast-analytics/0.1"},
        ) as http_client:
            client = OpenMeteoClient(http_client)

            forecast = client.get_forecast(
                latitude=args.latitude,
                longitude=args.longitude,
            )
    except (httpx.HTTPError, ValueError) as exc:
        parser.exit(1, f"Failed to collect forecast: {exc}\n")

    collected_at = datetime.now(UTC).isoformat()

    message = {
        "source": "open_meteo",
        "city_id": args.city_id,
        "requested_at_utc": requested_at,
        "collected_at_utc": collected_at,
        "request": {
            "url": FORECAST_URL,
            "parameters": build_forecast_params(
                args.latitude,
                args.longitude,
            ),
        },
        "payload": forecast,
    }

    json.dump(message, sys.stdout, ensure_ascii=False)
    sys.stdout.write("\n")


if __name__ == "__main__":
    main()
