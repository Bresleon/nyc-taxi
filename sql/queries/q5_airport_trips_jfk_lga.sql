SELECT
    d.year AS anio,
    CASE
        WHEN pz.zone_name IN ('JFK Airport', 'LaGuardia Airport')
         AND dz.zone_name IN ('JFK Airport', 'LaGuardia Airport')
            THEN 'Inter-Aeropuerto (JFK <-> LGA)'
        WHEN pz.zone_name = 'JFK Airport' OR dz.zone_name = 'JFK Airport'
            THEN 'JFK Airport'
        WHEN pz.zone_name = 'LaGuardia Airport' OR dz.zone_name = 'LaGuardia Airport'
            THEN 'LaGuardia Airport'
    END AS aeropuerto,
    COUNT(*) AS cantidad_viajes,
    ROUND(AVG(f.fare_amount), 2) AS tarifa_base_media_usd,
    ROUND(AVG(f.total_amount), 2) AS importe_total_medio_usd
FROM fact_trips f
JOIN dim_zone pz ON f.pu_location_id = pz.location_id
JOIN dim_zone dz ON f.do_location_id = dz.location_id
JOIN dim_date d ON f.pickup_date_key = d.date_key
WHERE pz.zone_name IN ('JFK Airport', 'LaGuardia Airport')
   OR dz.zone_name IN ('JFK Airport', 'LaGuardia Airport')
GROUP BY 1, 2
ORDER BY 1, 2;
