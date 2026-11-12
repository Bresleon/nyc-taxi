WITH stats_by_year AS (
    SELECT
        d.year,
        COUNT(*) AS total_trips,
        ROUND(AVG(f.fare_amount), 2) AS avg_fare_usd,
        ROUND(AVG(f.total_amount), 2) AS avg_total_usd,
        ROUND(AVG(f.trip_duration_minutes), 2) AS avg_duration_min,
        ROUND(AVG(f.avg_speed_mph), 2) AS avg_speed_mph
    FROM fact_trips f
    JOIN dim_date d ON f.pickup_date_key = d.date_key
    GROUP BY d.year
)
SELECT
    s24.total_trips AS viajes_2024,
    s25.total_trips AS viajes_2025,
    ROUND(((s25.total_trips - s24.total_trips) * 100.0) / NULLIF(s24.total_trips, 0), 2) AS var_viajes_pct,
    s24.avg_fare_usd AS tarifa_media_2024_usd,
    s25.avg_fare_usd AS tarifa_media_2025_usd,
    ROUND(((s25.avg_fare_usd - s24.avg_fare_usd) * 100.0) / NULLIF(s24.avg_fare_usd, 0), 2) AS var_tarifa_pct,
    s24.avg_total_usd AS total_medio_2024_usd,
    s25.avg_total_usd AS total_medio_2025_usd,
    ROUND(((s25.avg_total_usd - s24.avg_total_usd) * 100.0) / NULLIF(s24.avg_total_usd, 0), 2) AS var_total_pct
FROM stats_by_year s24
CROSS JOIN stats_by_year s25
WHERE s24.year = 2024 AND s25.year = 2025;
