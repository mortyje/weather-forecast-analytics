SELECT
    location_id,
    valid_from,
    COUNT(*) AS assignment_count
FROM {{ ref('core_location_station_assignments') }}
GROUP BY location_id, valid_from
HAVING COUNT(*) > 1
