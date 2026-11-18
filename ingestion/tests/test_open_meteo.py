import json

import pytest

from weather_ingestion.locations import load_locations


def test_load_locations(tmp_path):
    config = [
        {
            "id": "moscow",
            "name": "Москва",
            "latitude": 55.75,
            "longitude": 37.62,
        }
    ]

    path = tmp_path / "locations.json"
    path.write_text(json.dumps(config), encoding="utf-8")

    locations = load_locations(path)

    assert len(locations) == 1
    assert locations[0].id == "moscow"
    assert locations[0].latitude == 55.75


def test_duplicate_location_ids(tmp_path):
    config = [
        {
            "id": "moscow",
            "name": "Москва",
            "latitude": 55.75,
            "longitude": 37.62,
        },
        {
            "id": "moscow",
            "name": "Другой город",
            "latitude": 56.0,
            "longitude": 38.0,
        },
    ]

    path = tmp_path / "locations.json"
    path.write_text(json.dumps(config), encoding="utf-8")

    with pytest.raises(ValueError, match="Duplicate location id"):
        load_locations(path)


@pytest.mark.parametrize("latitude", [91, -91, True, "55.75"])
def test_invalid_latitude(tmp_path, latitude):
    config = [
        {
            "id": "moscow",
            "name": "Москва",
            "latitude": latitude,
            "longitude": 37.62,
        }
    ]

    path = tmp_path / "locations.json"
    path.write_text(json.dumps(config), encoding="utf-8")

    with pytest.raises(ValueError, match="Invalid latitude"):
        load_locations(path)
