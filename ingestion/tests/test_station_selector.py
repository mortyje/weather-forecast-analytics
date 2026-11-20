import pytest

from weather_ingestion.observations.selector import StationCandidate, select_station


def make_candidate(
    station_id: str,
    distance_km: float,
    temperature: float = 1.0,
    humidity: float = 1.0,
    wind: float = 1.0,
) -> StationCandidate:
    return StationCandidate(
        station_id=station_id,
        name=f"Station {station_id}",
        latitude=55.0,
        longitude=37.0,
        distance_km=distance_km,
        temperature_coverage=temperature,
        humidity_coverage=humidity,
        wind_coverage=wind,
        precipitation_coverage=0.0,
    )


def test_select_nearest_suitable_station():
    candidates = [
        make_candidate("A", 10, temperature=0.50),
        make_candidate("B", 25, temperature=0.95),
        make_candidate("C", 40, temperature=1.00),
    ]

    selected = select_station(candidates)

    assert selected is not None
    assert selected.station_id == "B"


def test_no_suitable_stations():
    candidates = [
        make_candidate("A", 5, humidity=0.50),
        make_candidate("B", 20, wind=0.70),
    ]

    assert select_station(candidates) is None


def test_empty_candidates():
    assert select_station([]) is None


def test_minimum_coverage_is_inclusive():
    candidates = [
        make_candidate(
            "A",
            10,
            temperature=0.90,
            humidity=0.90,
            wind=0.90,
        )
    ]

    selected = select_station(candidates)

    assert selected is not None
    assert selected.station_id == "A"


def test_precipitation_not_required():
    candidates = [make_candidate("A", 10)]

    selected = select_station(candidates)

    assert selected is not None
    assert selected.station_id == "A"


def test_selection_is_deterministic():
    candidates = [
        make_candidate("B", 10),
        make_candidate("A", 10),
    ]

    selected = select_station(candidates)

    assert selected is not None
    assert selected.station_id == "A"


@pytest.mark.parametrize("threshold", [-0.1, 1.1])
def test_invalid_coverage_threshold(threshold):
    with pytest.raises(ValueError, match="min_coverage"):
        select_station([], min_coverage=threshold)
