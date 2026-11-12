WITH global_metric AS (
    SELECT
        COUNT(*) AS total_trips_2025,
        SUM(CASE WHEN f.cbd_congestion_fee > 0 THEN 1 ELSE 0 END) AS trips_with_cbd_fee,
        ROUND(
            SUM(CASE WHEN f.cbd_congestion_fee > 0 THEN 1 ELSE 0 END) * 100.0 / NULLIF(COUNT(*), 0),
            2
        ) AS global_pct_with_cbd_fee
    FROM fact_trips f
    JOIN dim_date d ON f.pickup_date_key = d.date_key
    WHERE d.year = 2025
),
zone_ranking AS (
    SELECT
        z.location_id,
        z.borough,
        z.zone_name,
        COUNT(*) AS total_viajes_zona,
        SUM(CASE WHEN f.cbd_congestion_fee > 0 THEN 1 ELSE 0 END) AS viajes_con_cbd_fee,
        ROUND(
            SUM(CASE WHEN f.cbd_congestion_fee > 0 THEN 1 ELSE 0 END) * 100.0 / NULLIF(COUNT(*), 0),
            2
        ) AS proporcion_cbd_fee_pct
    FROM fact_trips f
    JOIN dim_zone z ON f.pu_location_id = z.location_id
    JOIN dim_date d ON f.pickup_date_key = d.date_key
    WHERE d.year = 2025
    GROUP BY 1, 2, 3
    HAVING COUNT(*) >= 100
)
SELECT
    g.global_pct_with_cbd_fee AS pct_global_mes_2025,
    r.location_id,
    r.borough,
    r.zone_name,
    r.total_viajes_zona,
    r.viajes_con_cbd_fee,
    r.proporcion_cbd_fee_pct
FROM zone_ranking r
CROSS JOIN global_metric g
ORDER BY r.proporcion_cbd_fee_pct DESC, r.total_viajes_zona DESC
LIMIT 5;
