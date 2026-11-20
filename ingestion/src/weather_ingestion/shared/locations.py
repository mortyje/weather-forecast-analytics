import json
import math
from dataclasses import dataclass
from pathlib import Path
from zoneinfo import ZoneInfo, ZoneInfoNotFoundError


@dataclass(frozen=True, slots=True)
class Location:
    id: str
    name: str
    latitude: float
    longitude: float
    timezone: str


def is_valid_coordinate(value: object, minimum: float, maximum: float) -> bool:
    return type(value) in (int, float) and math.isfinite(value) and minimum <= value <= maximum


def load_locations(path: Path) -> list[Location]:
    data = json.loads(path.read_text(encoding="utf-8"))

    if not isinstance(data, list) or not data:
        raise ValueError("Locations config must be a non-empty list")

    locations = []
    seen_ids = set()

    for item in data:
        if not isinstance(item, dict):
            raise ValueError("Each location must be an object")

        location_id = item.get("id")
        name = item.get("name")
        latitude = item.get("latitude")
        longitude = item.get("longitude")
        timezone = item.get("timezone")

        if not isinstance(location_id, str) or not location_id.strip():
            raise ValueError("Location must have a non-empty id")

        if location_id in seen_ids:
            raise ValueError(f"Duplicate location id: {location_id}")

        if not isinstance(name, str) or not name.strip():
            raise ValueError(f"Invalid name for location: {location_id}")

        if not is_valid_coordinate(latitude, -90, 90):
            raise ValueError(f"Invalid latitude for location: {location_id}")

        if not is_valid_coordinate(longitude, -180, 180):
            raise ValueError(f"Invalid longitude for location: {location_id}")

        if not isinstance(timezone, str) or not timezone.strip():
            raise ValueError(f"Missing timezone for location: {location_id}")

        try:
            ZoneInfo(timezone)
        except (ZoneInfoNotFoundError, ValueError) as exc:
            raise ValueError(f"Invalid timezone for location: {location_id}") from exc

        locations.append(
            Location(
                id=location_id,
                name=name,
                latitude=float(latitude),
                longitude=float(longitude),
                timezone=timezone,
            )
        )

        seen_ids.add(location_id)

    return locations
