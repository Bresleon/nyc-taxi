CREATE OR REPLACE TABLE dim_zone AS
SELECT
    location_id,
    borough,
    zone_name,
    service_zone,
    CASE
        WHEN zone_name IN ('JFK Airport', 'LaGuardia Airport', 'Newark Airport') THEN TRUE
        ELSE FALSE
    END AS is_airport
FROM raw_taxi_zones
ORDER BY location_id;
