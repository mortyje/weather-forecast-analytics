import pandas as pd

from weather_ingestion.observations.collection import (
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
