import json
import subprocess
from pathlib import Path
from typing import Any, Dict, List

from src.config import (
    PROJECT_ROOT,
    RESULTADOS_PATH,
    SQL_DIR,
    WAREHOUSE_PATH,
    MonthPeriod,
    get_duckdb_connection,
)


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
    FROM marts.fact_trips f
    JOIN marts.dim_zone z ON f.pu_location_id = z.location_id
    JOIN marts.dim_date d ON f.pickup_date_key = d.date_key
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
                "evidenciando la dinámica interanual traccionada por la movilidad en el Distrito Comercial Central."
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
FROM marts.agg_zone_hour
GROUP BY pickup_hour
ORDER BY ingreso_promedio_por_viaje_usd DESC
LIMIT 5;
""".strip(),
            "interpretation": (
                "La franja de las 05:00 AM (seguida de las 04:00 AM y 06:00 AM) registra el mayor ingreso promedio "
                "por viaje debido a que caen al mínimo los traslados cortos intra-Manhattan y predominan los "
                "viajes largos de tarifa alta hacia los aeropuertos JFK y LaGuardia."
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
    FROM marts.fact_trips f
    JOIN marts.dim_date d ON f.pickup_date_key = d.date_key
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
    FROM marts.fact_trips f
    JOIN marts.dim_zone z ON f.pu_location_id = z.location_id
    JOIN marts.dim_date d ON f.pickup_date_key = d.date_key
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
                "Cerca del 80% de los viajes en 2025 abonan el cargo cbd_congestion_fee (>0), y las 5 zonas con "
                "mayor proporción (>97%) se ubican dentro del área regulada al sur de la calle 60 en Manhattan, "
                "filtrando zonas con al menos 100 viajes para evitar sesgos de muestra pequeña."
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
FROM marts.fact_trips f
WHERE f.fare_amount > 0
GROUP BY 1, 2
ORDER BY cantidad_viajes DESC;
""".strip(),
            "interpretation": (
                "El valor de Efectivo (payment_type = 2 - Cash, ~0.00%) NO ES INTERPRETABLE porque, según el "
                "diccionario de datos de la TLC, las propinas pagadas en billetes físicos al conductor no quedan "
                "registradas en el taxímetro (censura en la captura del dato), generando un falso cero contable."
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
FROM marts.fact_trips f
JOIN marts.dim_zone pz ON f.pu_location_id = pz.location_id
JOIN marts.dim_zone dz ON f.do_location_id = dz.location_id
JOIN marts.dim_date d ON f.pickup_date_key = d.date_key
WHERE pz.zone_name IN ('JFK Airport', 'LaGuardia Airport')
   OR dz.zone_name IN ('JFK Airport', 'LaGuardia Airport')
GROUP BY 1, 2
ORDER BY 1, 2;
""".strip(),
            "interpretation": (
                "JFK registra una tarifa base media cercana a los $70 USD por la tarifa plana regulada entre JFK "
                "y Manhattan, mientras que LaGuardia opera por taxímetro con una tarifa base media menor debido "
                "a su mayor cercanía geográfica a Midtown."
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
    FROM marts.fact_trips f
    JOIN marts.dim_date d ON f.pickup_date_key = d.date_key
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
                "Hipótesis de causa (<= 5 líneas): La variación interanual entre 2024 y 2025 responde "
                "principalmente a la entrada en vigor del Peaje por Congestión (cbd_congestion_fee) al sur de "
                "la calle 60 en enero de 2025, que desincentivó el uso de vehículos particulares desplazando "
                "demanda hacia los taxis amarillos, junto con el recargo directo de $0.75 sobre el total y "
                "el crecimiento de viajes despachados por apps (Flex Fare)."
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


def run_dbt_transformations(periods: List[MonthPeriod]) -> None:
    """
    Ejecuta 'dbt run' y 'dbt test' para materializar los esquemas staging,
    intermediate y marts en DuckDB a partir de la capa curada Parquet.
    """
    dbt_dir = PROJECT_ROOT / "dbt-project"
    dbt_vars = json.dumps(
        {
            "target_month": periods[-1].month,
            "years": [p.year for p in periods],
        }
    )

    print(
        "\n[DBT] Materializando capas staging (tables), intermediate (views) y marts (tables)..."
    )
    res_run = subprocess.run(
        ["dbt", "run", "--project-dir", ".", "--profiles-dir", ".", "--vars", dbt_vars],
        cwd=dbt_dir,
        capture_output=True,
        text=True,
    )
    if res_run.returncode != 0:
        print(res_run.stdout)
        print(res_run.stderr)
        raise RuntimeError("Fallo crítico durante 'dbt run'.")
    print(res_run.stdout)

    print("[DBT] Ejecutando pruebas de integridad dimensional (dbt test)...")
    res_test = subprocess.run(
        [
            "dbt",
            "test",
            "--project-dir",
            ".",
            "--profiles-dir",
            ".",
            "--vars",
            dbt_vars,
        ],
        cwd=dbt_dir,
        capture_output=True,
        text=True,
    )
    if res_test.returncode != 0:
        print(res_test.stdout)
        print(res_test.stderr)
        raise RuntimeError(
            "Fallo crítico en los tests de integridad de dbt ('dbt test')."
        )
    print(res_test.stdout)


def run_business_queries_and_export_md(
    periods: List[MonthPeriod],
    warehouse_path: Path = WAREHOUSE_PATH,
    sql_dir: Path = SQL_DIR,
    resultados_path: Path = RESULTADOS_PATH,
) -> Dict[str, int]:
    """
    Abre conexión a DuckDB (una vez que dbt liberó el archivo), consulta las tablas
    dimensionales en el esquema 'marts', guarda los .sql y escribe RESULTADOS.md.
    """
    queries_dir = sql_dir / "queries"
    queries_dir.mkdir(parents=True, exist_ok=True)

    con = get_duckdb_connection(warehouse_path)
    try:
        counts = {
            "fact_trips": con.execute(
                "SELECT COUNT(*) FROM marts.fact_trips;"
            ).fetchone()[0],
            "agg_zone_hour": con.execute(
                "SELECT COUNT(*) FROM marts.agg_zone_hour;"
            ).fetchone()[0],
            "dim_zone": con.execute("SELECT COUNT(*) FROM marts.dim_zone;").fetchone()[
                0
            ],
            "dim_date": con.execute("SELECT COUNT(*) FROM marts.dim_date;").fetchone()[
                0
            ],
        }

        print(
            f"[MODELO ESTRELLA DBT] Verificado en schema 'marts' -> "
            f"fact_trips: {counts['fact_trips']:,} filas | "
            f"agg_zone_hour: {counts['agg_zone_hour']:,} filas | "
            f"dim_zone: {counts['dim_zone']} zonas | dim_date: {counts['dim_date']} días."
        )

        year_prev = periods[0].year
        year_curr = periods[-1].year
        queries = get_business_queries(year_prev, year_curr)

        md_sections = [
            f"# Resultados de Negocio — Pipeline NYC Yellow Taxi ({periods[0].label} vs. {periods[-1].label})\n",
            "Modelo dimensional estrella materializado con **dbt Core (`dbt-duckdb`)** organizado en los esquemas `staging`, `intermediate` y `marts`.\n"
            f"- **Total de viajes modelados en `marts.fact_trips`:** `{counts['fact_trips']:,}`\n"
            f"- **Total de registros horarios por zona en `marts.agg_zone_hour`:** `{counts['agg_zone_hour']:,}`\n",
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
        return counts
    finally:
        con.close()
