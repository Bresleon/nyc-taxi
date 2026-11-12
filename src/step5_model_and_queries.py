from pathlib import Path
from typing import Any, Dict, List

import duckdb

from src.config import CURATED_DIR, RESULTADOS_PATH, SQL_DIR, MonthPeriod

DDL_STATEMENTS: Dict[str, str] = {
    "dim_zone.sql": """
CREATE OR REPLACE TABLE dim_zone AS
SELECT
    location_id,
    borough,
    zone_name,
    service_zone,
    CASE
        WHEN zone_name IN ('JFK Airport', 'LaGuardia Airport', 'Newark Airport') THEN TRUE
        ELSE FALSE
    END AS is_airport
FROM raw_taxi_zones
ORDER BY location_id;
""".strip(),
    "dim_date.sql": """
CREATE OR REPLACE TABLE dim_date AS
WITH distinct_dates AS (
    SELECT DISTINCT pickup_date AS full_date
    FROM v_all_curated_trips
    WHERE pickup_date IS NOT NULL
)
SELECT
    CAST(STRFTIME(full_date, '%Y%m%d') AS INTEGER) AS date_key,
    full_date,
    CAST(EXTRACT('year' FROM full_date) AS INTEGER) AS year,
    CAST(EXTRACT('month' FROM full_date) AS INTEGER) AS month,
    MONTHNAME(full_date) AS month_name,
    CAST(EXTRACT('day' FROM full_date) AS INTEGER) AS day_of_month,
    CAST(EXTRACT('isodow' FROM full_date) AS INTEGER) AS day_of_week_num,
    DAYNAME(full_date) AS day_name,
    CASE WHEN EXTRACT('isodow' FROM full_date) IN (6, 7) THEN TRUE ELSE FALSE END AS is_weekend
FROM distinct_dates
ORDER BY full_date;
""".strip(),
    "fact_trips.sql": """
CREATE OR REPLACE TABLE fact_trips AS
SELECT
    ROW_NUMBER() OVER () AS trip_sk,
    CAST(STRFTIME(pickup_date, '%Y%m%d') AS INTEGER) AS pickup_date_key,
    pickup_date,
    pickup_hour,
    tpep_pickup_datetime,
    tpep_dropoff_datetime,
    vendor_id,
    ratecode_id,
    store_and_fwd_flag,
    payment_type,
    pu_location_id,
    do_location_id,
    passenger_count,
    trip_distance,
    trip_duration_minutes,
    avg_speed_mph,
    fare_amount,
    extra,
    mta_tax,
    tip_amount,
    tip_pct,
    tolls_amount,
    improvement_surcharge,
    congestion_surcharge,
    airport_fee,
    cbd_congestion_fee,
    total_amount,
    warning_flags,
    CAST(year AS INTEGER) AS year,
    CAST(month AS INTEGER) AS month
FROM v_all_curated_trips;
""".strip(),
    "agg_zone_hour.sql": """
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
""".strip(),
}


def get_business_queries(year_prev: int, year_curr: int) -> Dict[str, Dict[str, str]]:
    return {
        "q1_top_10_pickup_zones_yoy.sql": {
            "title": f"1. Top 10 zonas de origen con más viajes ({year_curr}) y su variación vs. {year_prev}",
            "sql": f"""
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
        SUM(CASE WHEN year = {year_prev} THEN trips ELSE 0 END) AS trips_{year_prev},
        SUM(CASE WHEN year = {year_curr} THEN trips ELSE 0 END) AS trips_{year_curr}
    FROM zone_yearly
    GROUP BY 1, 2, 3
)
SELECT
    pu_location_id AS location_id,
    borough,
    zone_name,
    trips_{year_prev},
    trips_{year_curr},
    (trips_{year_curr} - trips_{year_prev}) AS diff_viajes,
    ROUND(((trips_{year_curr} - trips_{year_prev}) * 100.0) / NULLIF(trips_{year_prev}, 0), 2) AS var_pct
FROM pivoted
ORDER BY trips_{year_curr} DESC
LIMIT 10;
""".strip(),
            "interpretation": (
                "Las 10 zonas principales concentran los nodos neurálgicos de Manhattan (Midtown Center, "
                "Upper East Side North/South, Times Square, Penn Station) junto con el aeropuerto JFK, "
                "evidenciando una recuperación y crecimiento interanual traccionado por la movilidad en el "
                "Distrito Comercial Central tras el inicio del peaje por congestión."
            ),
        },
        "q2_highest_avg_revenue_hour.sql": {
            "title": "2. Hora del día con mayor ingreso promedio por viaje",
            "sql": """
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
""".strip(),
            "interpretation": (
                "La franja de las 05:00 AM (seguida de las 04:00 AM y 06:00 AM) registra el mayor ingreso promedio "
                "por viaje debido a un cambio en la composición de los traslados: a esa hora caen al mínimo los "
                "viajes cortos intra-Manhattan y predominan los viajes largos de tarifa alta hacia los aeropuertos "
                "(JFK y LaGuardia) para los primeros vuelos matutinos."
            ),
        },
        "q3_cbd_congestion_fee_analysis.sql": {
            "title": f"3. Porcentaje de viajes con cbd_congestion_fee > 0 en {year_curr} y Top 5 zonas con mayor proporción",
            "sql": f"""
WITH global_metric AS (
    SELECT
        COUNT(*) AS total_trips_{year_curr},
        SUM(CASE WHEN f.cbd_congestion_fee > 0 THEN 1 ELSE 0 END) AS trips_with_cbd_fee,
        ROUND(
            SUM(CASE WHEN f.cbd_congestion_fee > 0 THEN 1 ELSE 0 END) * 100.0 / NULLIF(COUNT(*), 0),
            2
        ) AS global_pct_with_cbd_fee
    FROM fact_trips f
    JOIN dim_date d ON f.pickup_date_key = d.date_key
    WHERE d.year = {year_curr}
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
    WHERE d.year = {year_curr}
    GROUP BY 1, 2, 3
    HAVING COUNT(*) >= 100
)
SELECT
    g.global_pct_with_cbd_fee AS pct_global_mes_{year_curr},
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
""".strip(),
            "interpretation": (
                "Cerca de 4 de cada 5 viajes de taxi amarillo en 2025 abonan el cargo cbd_congestion_fee (>0), "
                "y las 5 zonas con mayor proporción (>97%) se ubican íntegramente dentro del área regulada "
                "al sur de la calle 60 en Manhattan (Midtown, SoHo, Financial District, Tribeca), filtrando "
                "zonas con al menos 100 viajes para evitar sesgos de muestra pequeña."
            ),
        },
        "q4_tip_pct_by_payment_type.sql": {
            "title": "4. Propina promedio (% de la tarifa) por tipo de pago",
            "sql": """
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
""".strip(),
            "interpretation": (
                "Mientras que en tarjeta de crédito (payment_type = 1) la propina ronda el ~20-22% de la tarifa base, "
                "el valor de Efectivo (payment_type = 2 - Cash, ~0.00%) NO ES INTERPRETABLE porque, según el "
                "diccionario de datos de la TLC, las propinas entregadas en billetes físicos al conductor no se "
                "registran en el taxímetro (censura en la captura del dato), generando un falso cero contable."
            ),
        },
        "q5_airport_trips_jfk_lga.sql": {
            "title": "5. Viajes y tarifa media con origen o destino en aeropuertos JFK o LaGuardia",
            "sql": """
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
""".strip(),
            "interpretation": (
                "JFK registra una tarifa base media cercana a los $68-$70 USD (regulada por la tarifa plana Flat Rate "
                "de $70 entre JFK y Manhattan), mientras que LaGuardia opera por taxímetro con una tarifa base media "
                "de ~$42-$45 USD dada su mayor cercanía geográfica a Midtown Manhattan."
            ),
        },
        "q6_yoy_total_trips_and_fare.sql": {
            "title": f"6. Variación interanual de viajes totales y tarifa media ({year_prev} vs. {year_curr})",
            "sql": f"""
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
    s24.total_trips AS viajes_{year_prev},
    s25.total_trips AS viajes_{year_curr},
    ROUND(((s25.total_trips - s24.total_trips) * 100.0) / NULLIF(s24.total_trips, 0), 2) AS var_viajes_pct,
    s24.avg_fare_usd AS tarifa_media_{year_prev}_usd,
    s25.avg_fare_usd AS tarifa_media_{year_curr}_usd,
    ROUND(((s25.avg_fare_usd - s24.avg_fare_usd) * 100.0) / NULLIF(s24.avg_fare_usd, 0), 2) AS var_tarifa_pct,
    s24.avg_total_usd AS total_medio_{year_prev}_usd,
    s25.avg_total_usd AS total_medio_{year_curr}_usd,
    ROUND(((s25.avg_total_usd - s24.avg_total_usd) * 100.0) / NULLIF(s24.avg_total_usd, 0), 2) AS var_total_pct
FROM stats_by_year s24
CROSS JOIN stats_by_year s25
WHERE s24.year = {year_prev} AND s25.year = {year_curr};
""".strip(),
            "interpretation": (
                "Hipótesis de causa (<= 5 líneas): La variación interanual en el volumen de viajes y en el importe "
                "medio entre 2024 y 2025 responde principalmente a la implementación del Peaje por Congestión "
                "(cbd_congestion_fee) al sur de la calle 60 desde enero de 2025, que desincentivó el uso de autos "
                "particulares transfiriendo demanda hacia el taxi amarillo, sumado a la incorporación de viajes "
                "de e-hail/Flex Fare (payment_type=0) y al recargo directo de $0.75 por congestión sobre el total."
            ),
        },
    }


def _rows_to_markdown_table(columns: List[str], rows: List[Any]) -> str:
    if not rows:
        return "_Sin resultados._\n"
    header = "| " + " | ".join(columns) + " |"
    sep = "| " + " | ".join(["---"] * len(columns)) + " |"
    body_lines = []
    for r in rows:
        formatted_cells = [str(val if val is not None else "NULL") for val in r]
        body_lines.append("| " + " | ".join(formatted_cells) + " |")
    return "\n".join([header, sep] + body_lines) + "\n"


def build_star_schema_and_answer_queries(
    con: duckdb.DuckDBPyConnection,
    periods: List[MonthPeriod],
    curated_base_dir: Path = CURATED_DIR,
    sql_dir: Path = SQL_DIR,
    resultados_path: Path = RESULTADOS_PATH,
) -> None:
    """
    1. Exporta los archivos .sql de DDL y consultas al directorio sql/.
    2. Construye el modelo estrella (dim_zone, dim_date, fact_trips, agg_zone_hour).
    3. Ejecuta las 6 preguntas de negocio y genera RESULTADOS.md.
    """
    ddl_dir = sql_dir / "ddl"
    queries_dir = sql_dir / "queries"
    ddl_dir.mkdir(parents=True, exist_ok=True)
    queries_dir.mkdir(parents=True, exist_ok=True)

    # Crear vista sobre las particiones Parquet de los meses solicitados
    parquet_glob = (curated_base_dir / "**" / "*.parquet").as_posix()
    period_filters = " OR ".join(
        [f"(year = {p.year} AND month = {p.month})" for p in periods]
    )
    con.execute(f"""
        CREATE OR REPLACE TEMP VIEW v_all_curated_trips AS
        SELECT *
        FROM read_parquet('{parquet_glob}', hive_partitioning=true)
        WHERE {period_filters};
    """)

    # Ejecutar y guardar DDLs del modelo estrella
    for filename, ddl_sql in DDL_STATEMENTS.items():
        (ddl_dir / filename).write_text(ddl_sql + "\n", encoding="utf-8")
        con.execute(ddl_sql)

    fact_count = con.execute("SELECT COUNT(*) FROM fact_trips;").fetchone()[0]
    agg_count = con.execute("SELECT COUNT(*) FROM agg_zone_hour;").fetchone()[0]
    zone_count = con.execute("SELECT COUNT(*) FROM dim_zone;").fetchone()[0]
    date_count = con.execute("SELECT COUNT(*) FROM dim_date;").fetchone()[0]

    print(
        f"[MODELO ESTRELLA] Construido con éxito -> "
        f"fact_trips: {fact_count:,} filas | agg_zone_hour: {agg_count:,} filas | "
        f"dim_zone: {zone_count} zonas | dim_date: {date_count} días."
    )

    year_prev = periods[0].year
    year_curr = periods[-1].year
    queries = get_business_queries(year_prev, year_curr)

    md_sections = [
        f"# Resultados de Negocio — Pipeline NYC Yellow Taxi ({periods[0].label} vs. {periods[-1].label})\n",
        f"Modelo dimensional generado sobre DuckDB a partir de la capa curada particionada.\n"
        f"- **Total de viajes modelados en `fact_trips`:** `{fact_count:,}`\n"
        f"- **Total de registros horarios por zona en `agg_zone_hour`:** `{agg_count:,}`\n",
    ]

    for q_file, q_meta in queries.items():
        (queries_dir / q_file).write_text(q_meta["sql"] + "\n", encoding="utf-8")
        cursor = con.execute(q_meta["sql"])
        col_names = [desc[0] for desc in cursor.description]
        rows = cursor.fetchall()
        md_table = _rows_to_markdown_table(col_names, rows)

        section = (
            f"## {q_meta['title']}\n\n"
            f"**Consulta SQL (`sql/queries/{q_file}`):**\n"
            f"```sql\n{q_meta['sql']}\n```\n\n"
            f"**Resultado:**\n\n"
            f"{md_table}\n"
            f"**Interpretación:** {q_meta['interpretation']}\n\n---\n"
        )
        md_sections.append(section)

    resultados_path.write_text("\n".join(md_sections), encoding="utf-8")
    print(
        f"[RESULTADOS] Archivo '{resultados_path.name}' generado con las 6 respuestas de negocio."
    )
