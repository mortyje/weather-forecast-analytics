from collections.abc import Iterable
from dataclasses import dataclass

MIN_COVERAGE = 0.90


@dataclass(frozen=True, slots=True)
class StationCandidate:
    station_id: str
    name: str
    latitude: float
    longitude: float
    distance_km: float
    temperature_coverage: float
    humidity_coverage: float
    wind_coverage: float
    precipitation_coverage: float


def select_station(
    candidates: Iterable[StationCandidate],
    min_coverage: float = MIN_COVERAGE,
) -> StationCandidate | None:
    if not 0 <= min_coverage <= 1:
        raise ValueError("min_coverage must be between 0 and 1")

    suitable_stations = [
        station
        for station in candidates
        if (
            station.temperature_coverage >= min_coverage
            and station.humidity_coverage >= min_coverage
            and station.wind_coverage >= min_coverage
        )
    ]

    if not suitable_stations:
        return None

    return min(
        suitable_stations,
        key=lambda station: (
            station.distance_km,
            -min(
                station.temperature_coverage,
                station.humidity_coverage,
                station.wind_coverage,
            ),
            station.station_id,
        ),
    )
