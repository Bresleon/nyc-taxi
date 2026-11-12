WITH zone_yearly AS (
    SELECT
        f.pu_location_id,
        z.borough,
        z.zone_name,
        d.year,
        COUNT(*) AS trips
    FROM fact_trips f
    JOIN dim_zone z ON f.pu_location_id = z.location_id
    JOIN dim_date d ON f.pickup_date_key = d.date_key
    GROUP BY 1, 2, 3, 4
),
pivoted AS (
    SELECT
        pu_location_id,
        borough,
        zone_name,
        SUM(CASE WHEN year = 2024 THEN trips ELSE 0 END) AS trips_2024,
        SUM(CASE WHEN year = 2025 THEN trips ELSE 0 END) AS trips_2025
    FROM zone_yearly
    GROUP BY 1, 2, 3
)
SELECT
    pu_location_id AS location_id,
    borough,
    zone_name,
    trips_2024,
    trips_2025,
    (trips_2025 - trips_2024) AS diff_viajes,
    ROUND(((trips_2025 - trips_2024) * 100.0) / NULLIF(trips_2024, 0), 2) AS var_pct
FROM pivoted
ORDER BY trips_2025 DESC
LIMIT 10;
