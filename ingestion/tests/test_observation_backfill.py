import json
import logging
import sys
from contextlib import nullcontext
from datetime import UTC, date, datetime
from pathlib import Path
from types import SimpleNamespace

import pandas as pd
import pytest

from weather_ingestion.observations import backfill
from weather_ingestion.observations.backfill import iter_time_windows, select_locations
from weather_ingestion.observations.collection import build_message


def test_single_window():
    assert list(iter_time_windows(date(2026, 10, 1), date(2026, 10, 3))) == [
        (datetime(2026, 10, 1), datetime(2026, 10, 4)),
    ]


def test_multiple_windows():
    assert list(iter_time_windows(date(2026, 10, 1), date(2026, 10, 16), chunk_days=7)) == [
        (datetime(2026, 10, 1), datetime(2026, 10, 8)),
        (datetime(2026, 10, 8), datetime(2026, 10, 15)),
        (datetime(2026, 10, 15), datetime(2026, 10, 17)),
    ]


def test_windows_have_no_gaps_or_overlaps():
    windows = list(iter_time_windows(date(2026, 10, 1), date(2026, 10, 31)))

    assert len(windows) == 5
    for current, following in zip(windows, windows[1:], strict=False):
        assert current[1] == following[0]


def test_invalid_date_range():
    with pytest.raises(ValueError):
        list(iter_time_windows(date(2026, 11, 10), date(2026, 11, 1)))


@pytest.mark.parametrize("chunk_days", [0, -1, 32])
def test_invalid_chunk_size(chunk_days):
    with pytest.raises(ValueError):
        list(iter_time_windows(date(2026, 10, 1), date(2026, 10, 7), chunk_days=chunk_days))


def test_select_single_city():
    moscow = SimpleNamespace(id="moscow")
    kazan = SimpleNamespace(id="kazan")

    assert select_locations([moscow, kazan], "kazan") == [kazan]


def test_unknown_city():
    with pytest.raises(ValueError, match="Unknown city"):
        select_locations([SimpleNamespace(id="moscow")], "unknown")


@pytest.fixture
def assignment():
    return {
        "station_id": "UUEE0",
        "station_name": "Moscow Airport",
        "station_latitude": 55.97,
        "station_longitude": 37.41,
        "selected_at": datetime(2026, 11, 20, 8, tzinfo=UTC),
    }


def test_backfill_message_mode(assignment):
    location = SimpleNamespace(
        id="moscow",
        name="Москва",
        latitude=55.75,
        longitude=37.62,
        timezone="Europe/Moscow",
    )

    message = build_message(
        location,
        assignment,
        observations=[],
        start=datetime(2026, 10, 1),
        end=datetime(2026, 10, 7, 23, 59, 59),
        ingestion_mode="backfill",
    )

    assert message["ingestion_mode"] == "backfill"
    assert message["schema_version"] == 2
    assert message["observation_window"]["start_utc"] == "2026-10-01T00:00:00+00:00"
    assert message["observation_window"]["end_utc"] == "2026-10-07T23:59:59+00:00"
    assert message["station"]["assigned_at_utc"] == "2026-11-20T08:00:00+00:00"


@pytest.fixture
def cli_args(monkeypatch):
    locations = Path(__file__).resolve().parents[1] / "config" / "locations.json"
    args = [
        "weather-observations-backfill",
        "--locations",
        str(locations),
        "--start",
        "2026-10-01",
        "--end",
        "2026-10-03",
    ]
    monkeypatch.setattr(sys, "argv", args)
    return args


def test_dry_run_has_no_external_calls(cli_args, monkeypatch, capsys):
    cli_args.extend(["--output", "kafka", "--dry-run"])

    def unexpected_call(*args, **kwargs):
        pytest.fail("Dry-run must not call external services")

    monkeypatch.setattr(backfill.psycopg, "connect", unexpected_call)
    monkeypatch.setattr(backfill, "configure_meteostat", unexpected_call)
    monkeypatch.setattr(backfill, "fetch_hourly_observations", unexpected_call)
    monkeypatch.setattr(backfill, "KafkaPublisher", unexpected_call)

    backfill.main()

    stdout, stderr = capsys.readouterr()
    assert len(stdout.splitlines()) == 5
    assert "moscow: [2026-10-01T00:00:00Z, 2026-10-04T00:00:00Z)" in stdout
    assert "Planned windows: 5" in stderr


@pytest.mark.parametrize("library_suppresses_error", [False, True])
def test_cli_continues_after_error_and_empty_window(
    cli_args, assignment, monkeypatch, capsys, library_suppresses_error
):
    cli_args.extend(["--city", "moscow", "--chunk-days", "1", "--sleep-seconds", "0"])
    monkeypatch.setattr(backfill.psycopg, "connect", lambda **kwargs: nullcontext(object()))
    monkeypatch.setattr(backfill, "get_assignment", lambda connection, city: assignment)
    monkeypatch.setattr(backfill, "assignment_matches_location", lambda assignment, city: True)

    def fetch(station_id, start, end):
        assert station_id == assignment["station_id"]
        assert end == datetime(2026, 10, start.day, 23, 59, 59)
        if start.day == 1:
            try:
                raise ConnectionError("Meteostat unavailable")
            except ConnectionError:
                if not library_suppresses_error:
                    raise
                logging.getLogger("meteostat").warning("Download failed", exc_info=True)
                return pd.DataFrame()
        if start.day == 2:
            logging.getLogger("meteostat").info("No data file was found")
            return pd.DataFrame()
        return pd.DataFrame({"temp": [12.0]}, index=pd.DatetimeIndex([start]))

    monkeypatch.setattr(backfill, "fetch_hourly_observations", fetch)

    with pytest.raises(SystemExit) as exit_info:
        backfill.main()

    assert exit_info.value.code == 1
    stdout, stderr = capsys.readouterr()
    message = json.loads(stdout)
    assert message["ingestion_mode"] == "backfill"
    assert message["observations"][0]["observed_at_utc"] == "2026-10-03T00:00:00+00:00"
    assert "exported=1, empty=1, failed=1" in stderr
