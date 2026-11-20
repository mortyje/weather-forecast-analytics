SELECT
    location_id,
    assignment_id,
    valid_from,
    valid_to
FROM {{ ref('core_location_station_assignments') }}
WHERE valid_to IS NOT NULL
    AND valid_to <= valid_from
