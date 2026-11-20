SELECT
    location_id,
    name,
    latitude,
    longitude,
    timezone
FROM {{ ref('core_locations') }}
