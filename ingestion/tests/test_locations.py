import json

import pytest

from weather_ingestion.shared.locations import load_locations


def make_location(**overrides) -> dict:
    return {
        "id": "moscow",
        "name": "Москва",
        "latitude": 55.75,
        "longitude": 37.62,
        "timezone": "Europe/Moscow",
        **overrides,
    }


def write_config(tmp_path, locations):
    path = tmp_path / "locations.json"
    path.write_text(json.dumps(locations), encoding="utf-8")
    return path


def test_load_locations(tmp_path):
    path = write_config(tmp_path, [make_location()])

    locations = load_locations(path)

    assert len(locations) == 1
    assert locations[0].id == "moscow"
    assert locations[0].latitude == 55.75
    assert locations[0].timezone == "Europe/Moscow"


def test_duplicate_location_ids(tmp_path):
    path = write_config(
        tmp_path,
        [
            make_location(),
            make_location(name="Другой город"),
        ],
    )

    with pytest.raises(ValueError, match="Duplicate location id"):
        load_locations(path)


@pytest.mark.parametrize("latitude", [91, -91, True, "55.75"])
def test_invalid_latitude(tmp_path, latitude):
    path = write_config(
        tmp_path,
        [make_location(latitude=latitude)],
    )

    with pytest.raises(ValueError, match="Invalid latitude"):
        load_locations(path)


@pytest.mark.parametrize("timezone", ["", None, 123])
def test_missing_timezone(tmp_path, timezone):
    path = write_config(
        tmp_path,
        [make_location(timezone=timezone)],
    )

    with pytest.raises(ValueError, match="Missing timezone"):
        load_locations(path)


def test_invalid_timezone(tmp_path):
    path = write_config(
        tmp_path,
        [make_location(timezone="Europe/UnknownCity")],
    )

    with pytest.raises(ValueError, match="Invalid timezone"):
        load_locations(path)
