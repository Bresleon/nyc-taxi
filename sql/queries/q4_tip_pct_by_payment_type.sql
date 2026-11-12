SELECT
    f.payment_type,
    CASE f.payment_type
        WHEN 0 THEN '0 - Flex Fare / App'
        WHEN 1 THEN '1 - Credit Card'
        WHEN 2 THEN '2 - Cash (Efectivo)'
        WHEN 3 THEN '3 - No Charge'
        WHEN 4 THEN '4 - Dispute'
        WHEN 5 THEN '5 - Unknown'
        WHEN 6 THEN '6 - Voided Trip'
        ELSE 'Otro'
    END AS descripcion_pago,
    COUNT(*) AS cantidad_viajes,
    ROUND(AVG(f.tip_pct), 2) AS propina_promedio_simple_pct,
    ROUND(SUM(f.tip_amount) * 100.0 / NULLIF(SUM(f.fare_amount), 0), 2) AS propina_ponderada_pct
FROM fact_trips f
WHERE f.fare_amount > 0
GROUP BY 1, 2
ORDER BY cantidad_viajes DESC;
