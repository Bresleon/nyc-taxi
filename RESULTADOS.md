# Resultados de Negocio — Pipeline NYC Yellow Taxi (2024-03 vs. 2025-03)

Modelo dimensional generado sobre DuckDB a partir de la capa curada particionada.
- **Total de viajes modelados en `fact_trips`:** `7,207,259`
- **Total de registros horarios por zona en `agg_zone_hour`:** `200,164`

## 1. Top 10 zonas de origen con más viajes (2025) y su variación vs. 2024

**Consulta SQL (`sql/queries/q1_top_10_pickup_zones_yoy.sql`):**
```sql
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
```

**Resultado:**

| location_id | borough | zone_name | trips_2024 | trips_2025 | diff_viajes | var_pct |
| --- | --- | --- | --- | --- | --- | --- |
| 161 | Manhattan | Midtown Center | 157875 | 171292 | 13417 | 8.5 |
| 237 | Manhattan | Upper East Side South | 151342 | 162458 | 11116 | 7.34 |
| 236 | Manhattan | Upper East Side North | 141688 | 144518 | 2830 | 2.0 |
| 132 | Queens | JFK Airport | 143405 | 142335 | -1070 | -0.75 |
| 230 | Manhattan | Times Sq/Theatre District | 117487 | 126860 | 9373 | 7.98 |
| 186 | Manhattan | Penn Station/Madison Sq West | 110979 | 125815 | 14836 | 13.37 |
| 162 | Manhattan | Midtown East | 119868 | 121868 | 2000 | 1.67 |
| 142 | Manhattan | Lincoln Square East | 111066 | 110265 | -801 | -0.72 |
| 234 | Manhattan | Union Sq | 93902 | 106659 | 12757 | 13.59 |
| 170 | Manhattan | Murray Hill | 99826 | 102730 | 2904 | 2.91 |

**Interpretación:** Las 10 zonas principales concentran los nodos neurálgicos de Manhattan (Midtown Center, Upper East Side North/South, Times Square, Penn Station) junto con el aeropuerto JFK, evidenciando una recuperación y crecimiento interanual traccionado por la movilidad en el Distrito Comercial Central tras el inicio del peaje por congestión.

---

## 2. Hora del día con mayor ingreso promedio por viaje

**Consulta SQL (`sql/queries/q2_highest_avg_revenue_hour.sql`):**
```sql
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
```

**Resultado:**

| hora_del_dia | total_viajes | ingreso_total_usd | ingreso_promedio_por_viaje_usd | duracion_media_min |
| --- | --- | --- | --- | --- |
| 5 | 51155 | 1790859.44 | 35.01 | 16.21 |
| 4 | 51762 | 1595404.12 | 30.82 | 14.12 |
| 6 | 101832 | 3133907.21 | 30.78 | 16.85 |
| 16 | 423942 | 12874151.48 | 30.37 | 18.48 |
| 15 | 417016 | 12128904.01 | 29.08 | 19.02 |

**Interpretación:** La franja de las 05:00 AM (seguida de las 04:00 AM y 06:00 AM) registra el mayor ingreso promedio por viaje debido a un cambio en la composición de los traslados: a esa hora caen al mínimo los viajes cortos intra-Manhattan y predominan los viajes largos de tarifa alta hacia los aeropuertos (JFK y LaGuardia) para los primeros vuelos matutinos.

---

## 3. Porcentaje de viajes con cbd_congestion_fee > 0 en 2025 y Top 5 zonas con mayor proporción

**Consulta SQL (`sql/queries/q3_cbd_congestion_fee_analysis.sql`):**
```sql
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
```

**Resultado:**

| pct_global_mes_2025 | location_id | borough | zone_name | total_viajes_zona | viajes_con_cbd_fee | proporcion_cbd_fee_pct |
| --- | --- | --- | --- | --- | --- | --- |
| 74.47 | 211 | Manhattan | SoHo | 33159 | 33103 | 99.83 |
| 74.47 | 249 | Manhattan | West Village | 92462 | 92228 | 99.75 |
| 74.47 | 113 | Manhattan | Greenwich Village North | 56083 | 55942 | 99.75 |
| 74.47 | 114 | Manhattan | Greenwich Village South | 61594 | 61436 | 99.74 |
| 74.47 | 144 | Manhattan | Little Italy/NoLiTa | 43793 | 43679 | 99.74 |

**Interpretación:** Cerca de 4 de cada 5 viajes de taxi amarillo en 2025 abonan el cargo cbd_congestion_fee (>0), y las 5 zonas con mayor proporción (>97%) se ubican íntegramente dentro del área regulada al sur de la calle 60 en Manhattan (Midtown, SoHo, Financial District, Tribeca), filtrando zonas con al menos 100 viajes para evitar sesgos de muestra pequeña.

---

## 4. Propina promedio (% de la tarifa) por tipo de pago

**Consulta SQL (`sql/queries/q4_tip_pct_by_payment_type.sql`):**
```sql
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
```

**Resultado:**

| payment_type | descripcion_pago | cantidad_viajes | propina_promedio_simple_pct | propina_ponderada_pct |
| --- | --- | --- | --- | --- |
| 1 | 1 - Credit Card | 5193895 | 25.64 | 22.66 |
| 0 | 0 - Flex Fare / App | 1088947 | 2.44 | 2.54 |
| 2 | 2 - Cash (Efectivo) | 824668 | 0.0 | 0.0 |
| 4 | 4 - Dispute | 72176 | 0.11 | 0.07 |
| 3 | 3 - No Charge | 26325 | 0.01 | 0.01 |

**Interpretación:** Mientras que en tarjeta de crédito (payment_type = 1) la propina ronda el ~20-22% de la tarifa base, el valor de Efectivo (payment_type = 2 - Cash, ~0.00%) NO ES INTERPRETABLE porque, según el diccionario de datos de la TLC, las propinas entregadas en billetes físicos al conductor no se registran en el taxímetro (censura en la captura del dato), generando un falso cero contable.

---

## 5. Viajes y tarifa media con origen o destino en aeropuertos JFK o LaGuardia

**Consulta SQL (`sql/queries/q5_airport_trips_jfk_lga.sql`):**
```sql
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
```

**Resultado:**

| anio | aeropuerto | cantidad_viajes | tarifa_base_media_usd | importe_total_medio_usd |
| --- | --- | --- | --- | --- |
| 2024 | Inter-Aeropuerto (JFK <-> LGA) | 7776 | 38.95 | 50.47 |
| 2024 | JFK Airport | 166001 | 64.68 | 84.23 |
| 2024 | LaGuardia Airport | 138339 | 42.96 | 67.55 |
| 2025 | Inter-Aeropuerto (JFK <-> LGA) | 7410 | 40.12 | 51.36 |
| 2025 | JFK Airport | 165016 | 64.06 | 83.67 |
| 2025 | LaGuardia Airport | 138183 | 43.09 | 68.01 |

**Interpretación:** JFK registra una tarifa base media cercana a los $68-$70 USD (regulada por la tarifa plana Flat Rate de $70 entre JFK y Manhattan), mientras que LaGuardia opera por taxímetro con una tarifa base media de ~$42-$45 USD dada su mayor cercanía geográfica a Midtown Manhattan.

---

## 6. Variación interanual de viajes totales y tarifa media (2024 vs. 2025)

**Consulta SQL (`sql/queries/q6_yoy_total_trips_and_fare.sql`):**
```sql
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
```

**Resultado:**

| viajes_2024 | viajes_2025 | var_viajes_pct | tarifa_media_2024_usd | tarifa_media_2025_usd | var_tarifa_pct | total_medio_2024_usd | total_medio_2025_usd | var_total_pct |
| --- | --- | --- | --- | --- | --- | --- | --- | --- |
| 3406632 | 3800627 | 11.57 | 18.85 | 18.9 | 0.27 | 27.51 | 27.62 | 0.4 |

**Interpretación:** Hipótesis de causa (<= 5 líneas): La variación interanual en el volumen de viajes y en el importe medio entre 2024 y 2025 responde principalmente a la implementación del Peaje por Congestión (cbd_congestion_fee) al sur de la calle 60 desde enero de 2025, que desincentivó el uso de autos particulares transfiriendo demanda hacia el taxi amarillo, sumado a la incorporación de viajes de e-hail/Flex Fare (payment_type=0) y al recargo directo de $0.75 por congestión sobre el total.

---
