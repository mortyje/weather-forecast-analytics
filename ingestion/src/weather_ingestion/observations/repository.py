from datetime import UTC, datetime
from decimal import Decimal

from psycopg import Connection

from weather_ingestion.observations.selector import StationCandidate
from weather_ingestion.shared.locations import Location


def get_assignment(
    connection: Connection,
    location_id: str,
) -> dict | None:
    cursor = connection.execute(
        """
        SELECT
            location_id,
            location_latitude,
            location_longitude,
            station_id,
            station_name
        FROM control.station_assignments
        WHERE location_id = %s
        """,
        (location_id,),
    )

    return cursor.fetchone()


def assignment_matches_location(
    assignment: dict,
    location: Location,
) -> bool:
    return assignment["location_latitude"] == Decimal(str(location.latitude)) and assignment[
        "location_longitude"
    ] == Decimal(str(location.longitude))


def save_assignment(
    connection: Connection,
    location: Location,
    station: StationCandidate,
    period_start: datetime,
    period_end: datetime,
) -> None:
    connection.execute(
        """
        INSERT INTO control.station_assignments (
            location_id,
            location_latitude,
            location_longitude,
            station_id,
            station_name,
            station_latitude,
            station_longitude,
            distance_km,
            temperature_coverage,
            humidity_coverage,
            wind_coverage,
            precipitation_coverage,
            evaluation_period_start,
            evaluation_period_end
        )
        VALUES (
            %s, %s, %s, %s, %s, %s, %s,
            %s, %s, %s, %s, %s, %s, %s
        )
        ON CONFLICT (location_id)
        DO UPDATE SET
            location_latitude = EXCLUDED.location_latitude,
            location_longitude = EXCLUDED.location_longitude,
            station_id = EXCLUDED.station_id,
            station_name = EXCLUDED.station_name,
            station_latitude = EXCLUDED.station_latitude,
            station_longitude = EXCLUDED.station_longitude,
            distance_km = EXCLUDED.distance_km,
            temperature_coverage = EXCLUDED.temperature_coverage,
            humidity_coverage = EXCLUDED.humidity_coverage,
            wind_coverage = EXCLUDED.wind_coverage,
            precipitation_coverage = EXCLUDED.precipitation_coverage,
            evaluation_period_start = EXCLUDED.evaluation_period_start,
            evaluation_period_end = EXCLUDED.evaluation_period_end,
            selected_at = NOW()
        """,
        (
            location.id,
            location.latitude,
            location.longitude,
            station.station_id,
            station.name,
            station.latitude,
            station.longitude,
            station.distance_km,
            station.temperature_coverage,
            station.humidity_coverage,
            station.wind_coverage,
            station.precipitation_coverage,
            period_start.replace(tzinfo=UTC),
            period_end.replace(tzinfo=UTC),
        ),
    )
