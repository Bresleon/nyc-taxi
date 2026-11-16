WITH yearly_agg AS (
    SELECT
        pu_location_id,
        pu_borough,
        pu_zone,
        year,
        SUM(total_trips) AS trips,
        SUM(cbd_congestion_trips) AS cbd_trips,
        SUM(total_revenue) AS revenue
    FROM {{ ref('int_zone_hourly_metrics') }}
    GROUP BY 1, 2, 3, 4
)
SELECT
    pu_location_id AS location_id,
    pu_borough AS borough,
    pu_zone AS zone_name,
    SUM(CASE WHEN year = 2024 THEN trips ELSE 0 END) AS trips_2024,
    SUM(CASE WHEN year = 2025 THEN trips ELSE 0 END) AS trips_2025,
    SUM(CASE WHEN year = 2025 THEN trips ELSE 0 END)
        - SUM(CASE WHEN year = 2024 THEN trips ELSE 0 END) AS diff_trips,
    ROUND(
        (SUM(CASE WHEN year = 2025 THEN trips ELSE 0 END)
         - SUM(CASE WHEN year = 2024 THEN trips ELSE 0 END)) * 100.0
        / NULLIF(SUM(CASE WHEN year = 2024 THEN trips ELSE 0 END), 0),
        2
    ) AS yoy_trips_var_pct,
    SUM(CASE WHEN year = 2025 THEN cbd_trips ELSE 0 END) AS cbd_trips_2025,
    ROUND(
        SUM(CASE WHEN year = 2025 THEN cbd_trips ELSE 0 END) * 100.0
        / NULLIF(SUM(CASE WHEN year = 2025 THEN trips ELSE 0 END), 0),
        2
    ) AS cbd_adoption_pct_2025,
    ROUND(SUM(CASE WHEN year = 2024 THEN revenue ELSE 0 END), 2) AS revenue_2024,
    ROUND(SUM(CASE WHEN year = 2025 THEN revenue ELSE 0 END), 2) AS revenue_2025
FROM yearly_agg
GROUP BY 1, 2, 3
