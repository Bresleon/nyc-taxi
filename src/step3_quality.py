"""
Paso 3 — Calidad de datos, cuarentena y reconciliación.

- Evalúa 9 reglas (7 obligatorias + 2 propias) en una sola pasada SQL en DuckDB.
- REJECT  -> la fila va a cuarentena (Parquet + tabla quarantine_trips) con las reglas violadas.
- WARNING -> la fila queda aceptada, con el ID de la regla en warning_flags.
- Verifica la invariante: filas_leídas == filas_aceptadas + filas_cuarentena.
"""

from dataclasses import dataclass
from datetime import datetime, timezone
from pathlib import Path
from typing import Dict, List, Tuple

import duckdb

from src.config import QUARANTINE_DIR, MonthPeriod


@dataclass(frozen=True)
class QualityRuleSpec:
    rule_id: str
    name: str
    severity: str  # 'REJECT' o 'WARNING'
    threshold_desc: str
    description: str


QUALITY_RULES: List[QualityRuleSpec] = [
    QualityRuleSpec(
        rule_id="R01_OUT_OF_MONTH_PICKUP",
        name="Fecha de subida fuera del mes del archivo",
        severity="REJECT",
        threshold_desc="YEAR(pickup) != año_archivo OR MONTH(pickup) != mes_archivo",
        description="El timestamp de subida no pertenece al año y mes nominal del archivo publicado.",
    ),
    QualityRuleSpec(
        rule_id="R02_DROPOFF_BEFORE_OR_EQUAL_PICKUP",
        name="Bajada anterior o igual a la subida",
        severity="REJECT",
        threshold_desc="tpep_dropoff_datetime <= tpep_pickup_datetime",
        description="Duración del viaje nula o negativa (imposibilidad física temporal).",
    ),
    QualityRuleSpec(
        rule_id="R03_INVALID_TRIP_DISTANCE",
        name="Distancia menor o igual a 0 o superior al máximo",
        severity="REJECT",
        threshold_desc="trip_distance <= 0.0 OR trip_distance > 200.0 millas",
        description="Viaje sin desplazamiento o con error de telemetría de odómetro (>200 mi).",
    ),
    QualityRuleSpec(
        rule_id="R04_NEGATIVE_TOTAL_OR_FARE",
        name="Importe total o tarifa base negativa",
        severity="REJECT",
        threshold_desc="total_amount < 0.0 OR fare_amount < 0.0",
        description="Contracargos, reversiones contables o errores de facturación con signo negativo.",
    ),
    QualityRuleSpec(
        rule_id="R05_NULL_OR_ZERO_PASSENGERS",
        name="Cantidad de pasajeros nula o igual a 0",
        severity="WARNING",
        threshold_desc="passenger_count IS NULL OR passenger_count <= 0",
        description="Falta de carga manual del chofer o viaje e-hail sin conteo de pasajeros.",
    ),
    QualityRuleSpec(
        rule_id="R06_UNKNOWN_TAXI_ZONE",
        name="Zona de origen o destino inexistente o desconocida",
        severity="REJECT",
        threshold_desc="pu/do_location_id fuera del lookup o igual a 264/265 (Unknown / N/A)",
        description="ID de zona inexistente en taxi_zone_lookup.csv o sin geolocalización (264, 265).",
    ),
    QualityRuleSpec(
        rule_id="R07_EXACT_DUPLICATE_ROW",
        name="Fila duplicada exacta",
        severity="REJECT",
        threshold_desc="row_number > 1 sobre todas las columnas del viaje",
        description="Retransmisión idéntica del mismo viaje; se conserva la primera instancia.",
    ),
    QualityRuleSpec(
        rule_id="R08_IMPOSSIBLE_DURATION_OR_SPEED",
        name="Duración mayor a 12h o velocidad media mayor a 100 mph (Regla Propia 1)",
        severity="REJECT",
        threshold_desc="duracion_min > 720.0 OR velocidad_mph > 100.0",
        description="Taxímetro olvidado abierto más de 12 horas o salto de GPS con velocidad >100 mph.",
    ),
    QualityRuleSpec(
        rule_id="R09_EXCESSIVE_PASSENGER_COUNT",
        name="Cantidad de pasajeros superior a la capacidad legal (Regla Propia 2)",
        severity="WARNING",
        threshold_desc="passenger_count > 6",
        description="Excede la capacidad máxima reglamentaria de taxis amarillos de la TLC.",
    ),
]

# Columnas del viaje (mismo orden que el contrato de esquema)
TRIP_COLUMNS: List[str] = [
    "vendor_id", "tpep_pickup_datetime", "tpep_dropoff_datetime",
    "passenger_count", "trip_distance", "ratecode_id", "store_and_fwd_flag",
    "pu_location_id", "do_location_id", "payment_type", "fare_amount",
    "extra", "mta_tax", "tip_amount", "tolls_amount", "improvement_surcharge",
    "total_amount", "congestion_surcharge", "airport_fee", "cbd_congestion_fee",
]


def init_quality_tables(con: duckdb.DuckDBPyConnection, zones_csv_path: Path) -> None:
    """Crea el catálogo de reglas, la tabla de zonas de referencia y las tablas de reporte."""
    con.execute("""
        CREATE TABLE IF NOT EXISTS quality_rules_catalog (
            rule_id VARCHAR PRIMARY KEY,
            name VARCHAR,
            severity VARCHAR,
            threshold_desc VARCHAR,
            description VARCHAR
        );
    """)
    for r in QUALITY_RULES:
        con.execute(
            "INSERT OR REPLACE INTO quality_rules_catalog VALUES (?, ?, ?, ?, ?);",
            [r.rule_id, r.name, r.severity, r.threshold_desc, r.description],
        )

    # Tabla maestra de zonas (la usa R06 acá y el enriquecimiento del paso 4)
    con.execute(f"""
        CREATE OR REPLACE TABLE raw_taxi_zones AS
        SELECT
            CAST(LocationID AS BIGINT) AS location_id,
            CAST(Borough AS VARCHAR) AS borough,
            CAST(Zone AS VARCHAR) AS zone_name,
            CAST(service_zone AS VARCHAR) AS service_zone
        FROM read_csv_auto('{zones_csv_path.as_posix()}', header=True);
    """)

    con.execute("""
        CREATE TABLE IF NOT EXISTS quality_summary_report (
            period VARCHAR PRIMARY KEY,
            source_file VARCHAR,
            rows_read BIGINT,
            rows_accepted BIGINT,
            rows_quarantined BIGINT,
            rows_with_warnings BIGINT,
            reconciliation_ok BOOLEAN,
            evaluated_at TIMESTAMP
        );
    """)

    con.execute("""
        CREATE TABLE IF NOT EXISTS quality_rule_counts (
            period VARCHAR,
            rule_id VARCHAR,
            severity VARCHAR,
            violated_count BIGINT,
            violated_pct DOUBLE,
            PRIMARY KEY (period, rule_id)
        );
    """)


def reconcile_records(
    period_label: str, rows_read: int, rows_accepted: int, rows_quarantined: int
) -> None:
    """
    Invariante obligatoria: filas_leidas == filas_aceptadas + filas_cuarentena.
    Si no se cumple, lanza RuntimeError y detiene el pipeline.
    """
    if rows_read != (rows_accepted + rows_quarantined):
        raise RuntimeError(
            f"[ERROR DE RECONCILIACIÓN en {period_label}] "
            f"filas_leídas ({rows_read}) != filas_aceptadas ({rows_accepted}) + "
            f"filas_cuarentena ({rows_quarantined}). Diferencia: "
            f"{rows_read - (rows_accepted + rows_quarantined)}"
        )


def run_quality_gate_for_month(
    con: duckdb.DuckDBPyConnection,
    period: MonthPeriod,
    standardized_view: str,
    zones_csv_path: Path,
    quarantine_base_dir: Path = QUARANTINE_DIR,
) -> Tuple[str, Dict[str, int]]:
    """
    Evalúa las 9 reglas sobre la vista estandarizada del mes (salida del paso 2).
    Devuelve (nombre de la vista de filas aceptadas, diccionario de conteos).
    """
    init_quality_tables(con, zones_csv_path)

    eval_view = f"v_evaluated_{period.year}_{period.month:02d}"
    accepted_view = f"v_accepted_{period.year}_{period.month:02d}"
    trip_cols_sql = ", ".join(TRIP_COLUMNS)
    partition_sql = ", ".join(f"s.{c}" for c in TRIP_COLUMNS)

    # Evaluación vectorizada de todas las reglas en una sola pasada.
    # COALESCE(cond, TRUE) en las REJECT: si la condición da NULL (dato faltante),
    # la fila se rechaza en vez de "escaparse" por la lógica trivalente de SQL.
    con.execute(f"""
        CREATE OR REPLACE TEMP VIEW {eval_view} AS
        WITH valid_geo_zones AS (
            SELECT location_id
            FROM raw_taxi_zones
            WHERE location_id IS NOT NULL
              AND location_id NOT IN (264, 265)
        ),
        dedup_tagged AS (
            SELECT
                s.*,
                ROW_NUMBER() OVER (
                    PARTITION BY {partition_sql}
                    ORDER BY s.tpep_pickup_datetime
                ) AS _dup_row_num,
                EPOCH(s.tpep_dropoff_datetime - s.tpep_pickup_datetime) / 60.0 AS _dur_min
            FROM {standardized_view} s
        ),
        rule_flags AS (
            SELECT
                d.*,
                -- Reglas REJECT
                COALESCE(
                    EXTRACT('year' FROM d.tpep_pickup_datetime) != {period.year}
                    OR EXTRACT('month' FROM d.tpep_pickup_datetime) != {period.month},
                    TRUE
                ) AS r01_violated,
                COALESCE(d.tpep_dropoff_datetime <= d.tpep_pickup_datetime, TRUE) AS r02_violated,
                COALESCE(d.trip_distance <= 0.0 OR d.trip_distance > 200.0, TRUE) AS r03_violated,
                COALESCE(d.total_amount < 0.0 OR d.fare_amount < 0.0, TRUE) AS r04_violated,
                COALESCE(
                    d.pu_location_id NOT IN (SELECT location_id FROM valid_geo_zones)
                    OR d.do_location_id NOT IN (SELECT location_id FROM valid_geo_zones),
                    TRUE
                ) AS r06_violated,
                (d._dup_row_num > 1) AS r07_violated,
                COALESCE(
                    d._dur_min > 720.0
                    OR (d._dur_min > 0 AND d.trip_distance / (d._dur_min / 60.0) > 100.0),
                    FALSE
                ) AS r08_violated,
                -- Reglas WARNING
                COALESCE(d.passenger_count <= 0, TRUE) AS r05_warning,
                COALESCE(d.passenger_count > 6, FALSE) AS r09_warning
            FROM dedup_tagged d
        )
        SELECT
            {trip_cols_sql},
            r01_violated, r02_violated, r03_violated, r04_violated,
            r05_warning, r06_violated, r07_violated, r08_violated, r09_warning,
            -- Listas armadas concatenando [] o ['ID'] por regla (sin lambdas,
            -- compatible con DuckDB 1.x y 2.x)
            (CASE WHEN r01_violated THEN ['R01_OUT_OF_MONTH_PICKUP'] ELSE CAST([] AS VARCHAR[]) END)
            || (CASE WHEN r02_violated THEN ['R02_DROPOFF_BEFORE_OR_EQUAL_PICKUP'] ELSE CAST([] AS VARCHAR[]) END)
            || (CASE WHEN r03_violated THEN ['R03_INVALID_TRIP_DISTANCE'] ELSE CAST([] AS VARCHAR[]) END)
            || (CASE WHEN r04_violated THEN ['R04_NEGATIVE_TOTAL_OR_FARE'] ELSE CAST([] AS VARCHAR[]) END)
            || (CASE WHEN r06_violated THEN ['R06_UNKNOWN_TAXI_ZONE'] ELSE CAST([] AS VARCHAR[]) END)
            || (CASE WHEN r07_violated THEN ['R07_EXACT_DUPLICATE_ROW'] ELSE CAST([] AS VARCHAR[]) END)
            || (CASE WHEN r08_violated THEN ['R08_IMPOSSIBLE_DURATION_OR_SPEED'] ELSE CAST([] AS VARCHAR[]) END)
            AS violated_reject_rules,
            (CASE WHEN r05_warning THEN ['R05_NULL_OR_ZERO_PASSENGERS'] ELSE CAST([] AS VARCHAR[]) END)
            || (CASE WHEN r09_warning THEN ['R09_EXCESSIVE_PASSENGER_COUNT'] ELSE CAST([] AS VARCHAR[]) END)
            AS warning_flags
        FROM rule_flags;
    """)

    # Cuarentena del mes en Parquet: data/quarantine/year=AAAA/month=MM/
    quarantine_dir = quarantine_base_dir / f"year={period.year}" / f"month={period.month:02d}"
    quarantine_dir.mkdir(parents=True, exist_ok=True)
    quarantine_file = quarantine_dir / f"quarantine_{period.label}.parquet"
    if quarantine_file.exists():
        quarantine_file.unlink()
    quarantine_sql_path = quarantine_file.as_posix()

    con.execute(f"""
        COPY (
            SELECT
                '{period.file_name}' AS source_file,
                '{period.label}' AS period,
                {trip_cols_sql},
                violated_reject_rules,
                warning_flags
            FROM {eval_view}
            WHERE len(violated_reject_rules) > 0
        ) TO '{quarantine_sql_path}' (FORMAT PARQUET, COMPRESSION ZSTD);
    """)

    # Copia en tabla DuckDB para consultarla fácil (se reemplaza solo el período procesado)
    con.execute(f"""
        CREATE TABLE IF NOT EXISTS quarantine_trips AS
        SELECT * FROM read_parquet('{quarantine_sql_path}') WHERE 1 = 0;
    """)
    con.execute("DELETE FROM quarantine_trips WHERE period = ?;", [period.label])
    con.execute(f"INSERT INTO quarantine_trips SELECT * FROM read_parquet('{quarantine_sql_path}');")

    # Vista de filas aceptadas (entrada del paso 4)
    con.execute(f"""
        CREATE OR REPLACE TEMP VIEW {accepted_view} AS
        SELECT {trip_cols_sql}, warning_flags
        FROM {eval_view}
        WHERE len(violated_reject_rules) = 0;
    """)

    # Conteos: leídas desde la vista del paso 2, cuarentena desde el archivo escrito
    counts_row = con.execute(f"""
        SELECT
            (SELECT COUNT(*) FROM {standardized_view}) AS rows_read,
            (SELECT COUNT(*) FROM {accepted_view}) AS rows_accepted,
            (SELECT COUNT(*) FROM read_parquet('{quarantine_sql_path}')) AS rows_quarantined,
            (SELECT COUNT(*) FROM {accepted_view} WHERE len(warning_flags) > 0) AS rows_with_warnings,
            SUM(CASE WHEN r01_violated THEN 1 ELSE 0 END),
            SUM(CASE WHEN r02_violated THEN 1 ELSE 0 END),
            SUM(CASE WHEN r03_violated THEN 1 ELSE 0 END),
            SUM(CASE WHEN r04_violated THEN 1 ELSE 0 END),
            SUM(CASE WHEN r05_warning  THEN 1 ELSE 0 END),
            SUM(CASE WHEN r06_violated THEN 1 ELSE 0 END),
            SUM(CASE WHEN r07_violated THEN 1 ELSE 0 END),
            SUM(CASE WHEN r08_violated THEN 1 ELSE 0 END),
            SUM(CASE WHEN r09_warning  THEN 1 ELSE 0 END)
        FROM {eval_view};
    """).fetchone()

    rows_read = int(counts_row[0] or 0)
    rows_accepted = int(counts_row[1] or 0)
    rows_quarantined = int(counts_row[2] or 0)
    rows_with_warnings = int(counts_row[3] or 0)
    rule_breakdown = {
        r.rule_id: int(counts_row[4 + i] or 0) for i, r in enumerate(QUALITY_RULES)
    }

    reconcile_records(period.label, rows_read, rows_accepted, rows_quarantined)

    now_utc = datetime.now(timezone.utc).replace(tzinfo=None)
    con.execute(
        "INSERT OR REPLACE INTO quality_summary_report VALUES (?, ?, ?, ?, ?, ?, ?, ?);",
        [
            period.label, period.file_name, rows_read, rows_accepted,
            rows_quarantined, rows_with_warnings, True, now_utc,
        ],
    )
    for r in QUALITY_RULES:
        c = rule_breakdown[r.rule_id]
        pct = round(c * 100.0 / rows_read, 4) if rows_read > 0 else 0.0
        con.execute(
            "INSERT OR REPLACE INTO quality_rule_counts VALUES (?, ?, ?, ?, ?);",
            [period.label, r.rule_id, r.severity, c, pct],
        )

    print(
        f"[CALIDAD {period.label}] Leídas: {rows_read:,} | "
        f"Aceptadas: {rows_accepted:,} ({rows_accepted * 100.0 / max(1, rows_read):.2f}%) | "
        f"Cuarentena: {rows_quarantined:,} ({rows_quarantined * 100.0 / max(1, rows_read):.2f}%) | "
        f"Con Warning: {rows_with_warnings:,}"
    )

    return accepted_view, {
        "rows_read": rows_read,
        "rows_accepted": rows_accepted,
        "rows_quarantined": rows_quarantined,
        **rule_breakdown,
    }
