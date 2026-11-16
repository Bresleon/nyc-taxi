SELECT
    location_id,
    borough,
    zone_name,
    service_zone,
    is_airport
FROM {{ ref('stg_tlc__taxi_zones') }}
ORDER BY location_id
