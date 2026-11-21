import json
import sys
from datetime import UTC, date, datetime
from pathlib import Path

import httpx
import pytest

from weather_ingestion.forecasts import backfill
from weather_ingestion.forecasts.backfill import (
    build_archive_message,
    fetch_model_run,
    iter_model_runs,
    select_locations,
    validate_forecast_response,
)
from weather_ingestion.shared.locations import Location


def make_location():
    return Location("moscow", "Москва", 55.75, 37.62, "Europe/Moscow")


def make_forecast():
    return {
        "hourly_units": {"temperature_2m": "°C", "wind_speed_10m": "km/h"},
        "hourly": {
            "time": ["2026-10-01T00:00", "2026-10-01T01:00"],
            "temperature_2m": [5.0, 4.8],
            "relative_humidity_2m": [80, 81],
            "precipitation": [None, 0.1],
            "wind_speed_10m": [10.0, 11.0],
        },
    }


def test_run_schedule_includes_first_and_last_day():
    runs = list(iter_model_runs(date(2026, 10, 1), date(2026, 10, 3)))

    assert len(runs) == 12
    assert runs[:4] == [datetime(2026, 10, 1, hour, tzinfo=UTC) for hour in (0, 6, 12, 18)]
    assert runs[-1] == datetime(2026, 10, 3, 18, tzinfo=UTC)


def test_rejects_reversed_dates():
    with pytest.raises(ValueError, match="Start date"):
        iter_model_runs(date(2026, 10, 3), date(2026, 10, 1))


def test_city_filter():
    location = make_location()
    locations = [location, Location("kazan", "Казань", 55.79, 49.12, "Europe/Moscow")]

    assert select_locations(locations, None) == locations
    assert select_locations(locations, "moscow") == [location]
    with pytest.raises(ValueError, match="Unknown city"):
        select_locations(locations, "unknown")


def test_fetch_uses_requested_run_and_model():
    forecast = make_forecast()

    def handler(request):
        assert str(request.url.copy_with(query=None)) == backfill.API_URL
        assert dict(request.url.params) == {
            "latitude": "55.75",
            "longitude": "37.62",
            "models": "ecmwf_ifs",
            "run": "2026-10-01T00:00",
            "forecast_hours": "168",
            "timezone": "GMT",
            "hourly": "temperature_2m,relative_humidity_2m,precipitation,wind_speed_10m",
            "temperature_unit": "celsius",
            "precipitation_unit": "mm",
            "wind_speed_unit": "kmh",
        }
        return httpx.Response(200, json=forecast)

    with httpx.Client(transport=httpx.MockTransport(handler)) as client:
        result = fetch_model_run(client, make_location(), datetime(2026, 10, 1, tzinfo=UTC))

    assert result == forecast


def test_preserves_missing_values_and_rejects_mismatched_arrays():
    forecast = make_forecast()
    assert validate_forecast_response(forecast) is forecast

    forecast["hourly"]["temperature_2m"].pop()
    with pytest.raises(ValueError, match="Invalid hourly series: temperature_2m"):
        validate_forecast_response(forecast)


def test_distinguishes_model_run_from_collection_time():
    forecast = make_forecast()
    before = datetime.now(UTC)
    message = build_archive_message(make_location(), datetime(2026, 10, 1, tzinfo=UTC), forecast)

    assert message["forecast_kind"] == "archived_run"
    assert message["model_id"] == "ecmwf_ifs"
    assert message["source"] == "open_meteo"
    assert message["city_id"] == message["location"]["id"] == "moscow"
    assert message["model_run_at_utc"] == "2026-10-01T00:00:00+00:00"
    assert before <= datetime.fromisoformat(message["collected_at_utc"]) <= datetime.now(UTC)
    assert message["forecast"] is forecast


@pytest.fixture
def cli_args(monkeypatch):
    path = Path(__file__).resolve().parents[1] / "config" / "locations.json"
    args = [
        "weather-forecasts-backfill",
        "--locations",
        str(path),
        "--start",
        "2026-10-01",
        "--end",
        "2026-10-03",
        "--sleep-seconds",
        "0",
    ]
    monkeypatch.setattr(sys, "argv", args)
    return args


def test_dry_run_avoids_http_and_kafka(cli_args, monkeypatch, capsys):
    cli_args.extend(["--dry-run", "--output", "kafka"])

    def unexpected_call(*args, **kwargs):
        pytest.fail("Dry-run opened a network client")

    monkeypatch.setattr(backfill.httpx, "Client", unexpected_call)
    monkeypatch.setattr(backfill, "KafkaPublisher", unexpected_call)

    backfill.main()

    stdout, stderr = capsys.readouterr()
    assert len(stdout.splitlines()) == 60
    assert "Planned requests: 60" in stderr


def test_cli_continues_when_a_run_is_unavailable(cli_args, monkeypatch, capsys):
    cli_args[cli_args.index("--end") + 1] = "2026-10-01"
    cli_args.extend(["--city", "moscow"])
    client = httpx.Client(
        transport=httpx.MockTransport(
            lambda request: (
                httpx.Response(404)
                if request.url.params["run"].endswith("T06:00")
                else httpx.Response(200, json=make_forecast())
            )
        )
    )
    monkeypatch.setattr(backfill.httpx, "Client", lambda **kwargs: client)

    with pytest.raises(SystemExit) as exc_info:
        backfill.main()

    assert exc_info.value.code == 1
    stdout, stderr = capsys.readouterr()
    assert [json.loads(line)["model_run_at_utc"][11:16] for line in stdout.splitlines()] == [
        "00:00",
        "12:00",
        "18:00",
    ]
    assert "404" in stderr
    assert "exported=3, failed=1" in stderr
