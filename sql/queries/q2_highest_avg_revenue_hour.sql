SELECT
    pickup_hour AS hora_del_dia,
    SUM(total_trips) AS total_viajes,
    ROUND(SUM(total_revenue), 2) AS ingreso_total_usd,
    ROUND(SUM(total_revenue) / NULLIF(SUM(total_trips), 0), 2) AS ingreso_promedio_por_viaje_usd,
    ROUND(SUM(avg_duration_minutes * total_trips) / NULLIF(SUM(total_trips), 0), 2) AS duracion_media_min
FROM agg_zone_hour
GROUP BY pickup_hour
ORDER BY ingreso_promedio_por_viaje_usd DESC
LIMIT 5;
