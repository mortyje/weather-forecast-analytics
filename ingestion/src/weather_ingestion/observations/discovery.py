import argparse
from datetime import datetime
from pathlib import Path

import psycopg
from psycopg.rows import dict_row

from weather_ingestion.observations.meteostat_client import (
    configure_meteostat,
    discover_candidates,
    get_observation_period,
)
from weather_ingestion.observations.repository import (
    assignment_matches_location,
    get_assignment,
    save_assignment,
)
from weather_ingestion.observations.selector import select_station
from weather_ingestion.shared.locations import Location, load_locations


def process_locations(
    locations: list[Location],
    start: datetime,
    end: datetime,
    connection: psycopg.Connection | None = None,
) -> int:
    failed_locations = 0

    for location in locations:
        if connection is not None:
            assignment = get_assignment(connection, location.id)

            if assignment is not None and assignment_matches_location(assignment, location):
                print(f"{location.id}: using existing station {assignment['station_id']}")
                continue

        try:
            candidates = discover_candidates(location, start, end)
        except Exception as exc:
            print(f"{location.id}: station discovery failed: {type(exc).__name__}: {exc}")
            failed_locations += 1
            continue

        selected = select_station(candidates)

        if selected is None:
            print(f"{location.id}: no station meets the minimum coverage requirements")
            failed_locations += 1
            continue

        print(
            f"\nSELECTED: {selected.station_id} | {selected.name} | {selected.distance_km:.1f} km"
        )

        if connection is not None:
            save_assignment(
                connection,
                location,
                selected,
                start,
                end,
            )
            print(f"SAVED: {location.id}")

    return failed_locations


def main() -> None:
    parser = argparse.ArgumentParser(description="Discover and select Meteostat stations")
    parser.add_argument(
        "--locations",
        type=Path,
        required=True,
    )
    parser.add_argument(
        "--assign",
        action="store_true",
        help="Save selected stations to PostgreSQL",
    )

    args = parser.parse_args()
    locations = load_locations(args.locations)

    configure_meteostat()
    start, end = get_observation_period()

    print("Meteostat station discovery")
    print("Model data: disabled")
    print(f"Period (UTC): {start} — {end}")

    if args.assign:
        with psycopg.connect(
            autocommit=True,
            row_factory=dict_row,
        ) as connection:
            failed_locations = process_locations(
                locations,
                start,
                end,
                connection,
            )
    else:
        failed_locations = process_locations(
            locations,
            start,
            end,
        )

    if failed_locations:
        raise SystemExit(f"Station discovery failed for {failed_locations} location(s)")


if __name__ == "__main__":
    main()
