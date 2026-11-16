SELECT
    CAST(LocationID AS BIGINT) AS location_id,
    TRIM(CAST(Borough AS VARCHAR)) AS borough,
    TRIM(CAST(Zone AS VARCHAR)) AS zone_name,
    COALESCE(NULLIF(TRIM(CAST(service_zone AS VARCHAR)), 'N/A'), 'Unknown') AS service_zone,
    CASE
        WHEN TRIM(CAST(Zone AS VARCHAR)) IN ('JFK Airport', 'LaGuardia Airport', 'Newark Airport') THEN TRUE
        ELSE FALSE
    END AS is_airport
FROM {{ source('reference', 'taxi_zones') }}
WHERE LocationID IS NOT NULL
