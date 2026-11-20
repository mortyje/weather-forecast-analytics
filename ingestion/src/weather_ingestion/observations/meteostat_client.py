from datetime import UTC, datetime, timedelta

import meteostat as ms

from weather_ingestion.observations.selector import StationCandidate
from weather_ingestion.shared.locations import Location

PERIOD_HOURS = 168
OBSERVATION_DELAY_HOURS = 48
STATION_RADIUS_METERS = 100_000
STATION_LIMIT = 5

WEATHER_PARAMETERS = {
    "temp": "temperature",
    "rhum": "humidity",
    "prcp": "precipitation",
    "wspd": "wind_speed",
}


def configure_meteostat() -> None:
    ms.config.cache_directory = "/tmp/meteostat-cache"
    ms.config.stations_db_file = "/tmp/meteostat-cache/stations.db"
    ms.config.include_model_data = False


def get_observation_period() -> tuple[datetime, datetime]:
    end = datetime.now(UTC).replace(minute=0, second=0, microsecond=0) - timedelta(
        hours=OBSERVATION_DELAY_HOURS
    )
    start = end - timedelta(hours=PERIOD_HOURS - 1)

    # Meteostat expects UTC timestamps without tzinfo
    # when timezone="UTC" is specified in the query.
    return (
        start.replace(tzinfo=None),
        end.replace(tzinfo=None),
    )


def calculate_coverage(data, column: str) -> float:
    if column not in data.columns:
        return 0.0

    available_hours = int(data[column].notna().sum())
    return min(1.0, available_hours / PERIOD_HOURS)


def discover_candidates(
    location: Location,
    start: datetime,
    end: datetime,
) -> list[StationCandidate]:
    print(f"\n=== {location.name} ({location.id}) ===")

    point = ms.Point(location.latitude, location.longitude)

    stations = ms.stations.nearby(
        point,
        radius=STATION_RADIUS_METERS,
        limit=STATION_LIMIT,
    )

    if stations.empty:
        print("No nearby weather stations found")
        return []

    candidates = []

    for station_id, station in stations.iterrows():
        station_id = str(station_id)
        station_name = str(station["name"])
        distance_km = float(station["distance"]) / 1000

        print(f"\nStation: {station_id} | {station_name} | {distance_km:.1f} km")

        try:
            timeseries = ms.hourly(
                ms.Station(id=station_id),
                start,
                end,
                timezone="UTC",
            )
            data = timeseries.fetch()

        except Exception as exc:
            print(f"Failed to retrieve observations: {type(exc).__name__}: {exc}")
            continue

        if data is None or data.empty:
            print("No hourly observations available")
            continue

        coverage = {column: calculate_coverage(data, column) for column in WEATHER_PARAMETERS}

        print(f"Returned rows: {len(data)}")

        for column, parameter_name in WEATHER_PARAMETERS.items():
            print(f"  {parameter_name:<16}{coverage[column] * 100:6.1f}%")

        candidates.append(
            StationCandidate(
                station_id=station_id,
                name=station_name,
                latitude=float(station["latitude"]),
                longitude=float(station["longitude"]),
                distance_km=distance_km,
                temperature_coverage=coverage["temp"],
                humidity_coverage=coverage["rhum"],
                wind_coverage=coverage["wspd"],
                precipitation_coverage=coverage["prcp"],
            )
        )

    return candidates
