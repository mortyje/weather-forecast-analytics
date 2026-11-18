from typing import Any

import httpx

FORECAST_URL = "https://api.open-meteo.com/v1/forecast"

HOURLY_VARIABLES = (
    "temperature_2m",
    "relative_humidity_2m",
    "precipitation",
    "wind_speed_10m",
)


def build_forecast_params(latitude: float, longitude: float) -> dict:
    return {
        "latitude": latitude,
        "longitude": longitude,
        "hourly": ",".join(HOURLY_VARIABLES),
        "forecast_hours": 168,
        "timezone": "UTC",
        "temperature_unit": "celsius",
        "precipitation_unit": "mm",
        "wind_speed_unit": "kmh",
    }


class OpenMeteoClient:
    def __init__(self, http_client: httpx.Client) -> None:
        self._http_client = http_client

    def get_forecast(self, latitude: float, longitude: float) -> dict[str, Any]:
        response = self._http_client.get(
            FORECAST_URL,
            params=build_forecast_params(latitude, longitude),
        )
        response.raise_for_status()

        payload = response.json()

        if not isinstance(payload, dict):
            raise ValueError("Invalid API response")

        hourly = payload.get("hourly")

        if not isinstance(hourly, dict):
            raise ValueError("Missing or invalid hourly data")

        timestamps = hourly.get("time")

        if (
            not isinstance(timestamps, list)
            or not timestamps
            or not all(isinstance(value, str) for value in timestamps)
        ):
            raise ValueError("Missing or invalid hourly timestamps")

        for variable in HOURLY_VARIABLES:
            values = hourly.get(variable)

            if not isinstance(values, list) or len(values) != len(timestamps):
                raise ValueError(f"Invalid hourly series: {variable}")

        return payload
