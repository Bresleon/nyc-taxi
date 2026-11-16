SELECT
    pu_location_id,
    pu_borough,
    pu_zone,
    pickup_date,
    pickup_hour,
    year,
    month,
    COUNT(*) AS total_trips,
    SUM(is_cbd_congestion_trip) AS cbd_congestion_trips,
    ROUND(SUM(total_amount), 2) AS total_revenue,
    ROUND(AVG(total_amount), 2) AS avg_revenue_per_trip,
    ROUND(AVG(fare_amount), 2) AS avg_fare_amount,
    ROUND(AVG(trip_duration_minutes), 2) AS avg_duration_minutes
FROM {{ ref('int_trips_enriched') }}
GROUP BY
    pu_location_id,
    pu_borough,
    pu_zone,
    pickup_date,
    pickup_hour,
    year,
    month
