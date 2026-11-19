import httpx
import pytest

from weather_ingestion.open_meteo import OpenMeteoClient


def make_payload() -> dict:
    return {
        "hourly": {
            "time": ["2026-11-18T12:00", "2026-11-18T13:00"],
            "temperature_2m": [2.5, 3.0],
            "relative_humidity_2m": [80, 82],
            "precipitation": [0.0, 0.2],
            "wind_speed_10m": [10.0, 12.0],
        }
    }


def test_get_forecast_success() -> None:
    expected_payload = make_payload()

    def handler(request: httpx.Request) -> httpx.Response:
        assert request.url.params["latitude"] == "55.75"
        assert request.url.params["longitude"] == "37.62"
        assert request.url.params["forecast_hours"] == "168"
        assert request.url.params["timezone"] == "UTC"

        return httpx.Response(200, json=expected_payload)

    transport = httpx.MockTransport(handler)

    with httpx.Client(transport=transport) as http_client:
        client = OpenMeteoClient(http_client)
        result = client.get_forecast(55.75, 37.62)

    assert result == expected_payload


def test_get_forecast_invalid_hourly_series() -> None:
    payload = make_payload()
    payload["hourly"]["temperature_2m"] = [2.5]

    transport = httpx.MockTransport(
        lambda request: httpx.Response(200, json=payload)
    )

    with httpx.Client(transport=transport) as http_client:
        client = OpenMeteoClient(http_client)

        with pytest.raises(ValueError, match="Invalid hourly series"):
            client.get_forecast(55.75, 37.62)


def test_get_forecast_http_error() -> None:
    transport = httpx.MockTransport(
        lambda request: httpx.Response(500)
    )

    with httpx.Client(transport=transport) as http_client:
        client = OpenMeteoClient(http_client)

        with pytest.raises(httpx.HTTPStatusError):
            client.get_forecast(55.75, 37.62)
