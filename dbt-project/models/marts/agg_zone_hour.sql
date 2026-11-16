SELECT
    pu_location_id,
    pu_borough,
    pu_zone,
    pickup_date,
    pickup_hour,
    year,
    month,
    total_trips,
    total_revenue,
    avg_revenue_per_trip,
    avg_fare_amount,
    avg_duration_minutes
FROM {{ ref('int_zone_hourly_metrics') }}
