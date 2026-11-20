from datetime import datetime, timedelta, timezone
from decimal import Decimal
from types import SimpleNamespace

import pandas as pd

from weather_ingestion.observations.collection import (
    build_message,
    numeric_or_none,
    serialize_observations,
)


def test_missing_value_becomes_none():
    assert numeric_or_none(float("nan")) is None
    assert numeric_or_none(pd.NA) is None


def test_zero_is_preserved():
    assert numeric_or_none(0.0) == 0.0


def test_serialize_observations():
    data = pd.DataFrame(
        {
            "temp": [3.2, 3.5],
            "rhum": [82, 80],
            "prcp": [float("nan"), 0.0],
            "wspd": [14.4, 12.0],
        },
        index=pd.DatetimeIndex(
            [
                "2026-11-18 10:00:00",
                "2026-11-18 11:00:00",
            ],
            name="time",
        ),
    )

    observations = serialize_observations(data)

    assert len(observations) == 2

    assert observations[0]["observed_at_utc"] == ("2026-11-18T10:00:00+00:00")
    assert observations[0]["temperature_c"] == 3.2
    assert observations[0]["precipitation_mm"] is None

    assert observations[1]["precipitation_mm"] == 0.0


def test_missing_parameter_becomes_none():
    data = pd.DataFrame(
        {"temp": [5.0]},
        index=pd.DatetimeIndex(
            ["2026-11-18 10:00:00"],
            name="time",
        ),
    )

    observations = serialize_observations(data)

    assert observations[0]["temperature_c"] == 5.0
    assert observations[0]["humidity_pct"] is None
    assert observations[0]["wind_speed_kmh"] is None


def test_observation_message_contains_station_metadata():
    location = SimpleNamespace(
        id="moscow",
        name="Москва",
        latitude=55.75,
        longitude=37.62,
        timezone="Europe/Moscow",
    )
    assignment = {
        "station_id": "UUEE0",
        "station_name": "Moscow Airport",
        "station_latitude": Decimal("55.97"),
        "station_longitude": Decimal("37.41"),
        "selected_at": datetime(2026, 11, 20, 11, 0, tzinfo=timezone(timedelta(hours=3))),
    }
    start = datetime(2026, 11, 11)
    end = datetime(2026, 11, 18)

    message = build_message(location, assignment, [], start, end)

    assert message["schema_version"] == 2
    assert message["station"] == {
        "id": "UUEE0",
        "name": "Moscow Airport",
        "latitude": 55.97,
        "longitude": 37.41,
        "assigned_at_utc": "2026-11-20T08:00:00+00:00",
    }
    assert isinstance(message["station"]["latitude"], float)
    assert isinstance(message["station"]["longitude"], float)
