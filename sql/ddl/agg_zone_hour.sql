CREATE OR REPLACE TABLE agg_zone_hour AS
SELECT
    f.pu_location_id,
    z.borough AS pu_borough,
    z.zone_name AS pu_zone,
    f.pickup_date,
    f.pickup_hour,
    f.year,
    f.month,
    COUNT(*) AS total_trips,
    ROUND(SUM(f.total_amount), 2) AS total_revenue,
    ROUND(AVG(f.total_amount), 2) AS avg_revenue_per_trip,
    ROUND(AVG(f.fare_amount), 2) AS avg_fare_amount,
    ROUND(AVG(f.trip_duration_minutes), 2) AS avg_duration_minutes
FROM fact_trips f
JOIN dim_zone z ON f.pu_location_id = z.location_id
GROUP BY
    f.pu_location_id,
    z.borough,
    z.zone_name,
    f.pickup_date,
    f.pickup_hour,
    f.year,
    f.month;
