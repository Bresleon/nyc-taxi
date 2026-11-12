from dataclasses import dataclass
from datetime import datetime, timezone
from pathlib import Path
from typing import Dict, List, Tuple
import duckdb

from src.config import MonthPeriod, QUARANTINE_DIR


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
        description="Falta de carga manual del chofer o viaje e-hail sin conteo de pasajeros (~10% del total).",
    ),
    QualityRuleSpec(
        rule_id="R06_UNKNOWN_TAXI_ZONE",
        name="Zona de origen o destino inexistente o desconocida",
        severity="REJECT",
        threshold_desc="pu_location_id / do_location_id fuera de catálogo válido (1..263)",
        description="ID de zona inexistente en taxi_zone_lookup.csv o marcado como N/A / Unknown (264, 265).",
    ),
    QualityRuleSpec(
        rule_id="R07_EXACT_DUPLICATE_ROW",
        name="Fila duplicada exacta",
        severity="REJECT",
        threshold_desc="row_instance_num > 1 sobre todas las columnas del evento",
        description="Retransmisión idéntica del mismo viaje desde el taxímetro.",
    ),
    QualityRuleSpec(
        rule_id="R08_IMPOSSIBLE_DURATION_OR_SPEED",
        name="Duración mayor a 12h o velocidad media mayor a 100 mph (Regla Propia 1)",
        severity="REJECT",
        threshold_desc="duracion_min > 720.0 OR velocidad_mph > 100.0",
        description="Taxímetro olvidado abierto por más de 12 horas o salto de GPS con velocidad >100 mph.",
    ),
    QualityRuleSpec(
        rule_id="R09_EXCESSIVE_PASSENGER_COUNT",
        name="Cantidad de pasajeros superior a la capacidad legal (Regla Propia 2)",
        severity="WARNING",
        threshold_desc="passenger_count > 6",
        description="Excede la capacidad máxima reglamentaria de taxis amarillos de la TLC (6 pasajeros).",
    ),
]


def init_quality_tables(con: duckdb.DuckDBPyConnection, zones_csv_path: Path) -> None:
    """Inicializa el catálogo de reglas, la tabla de zonas de referencia y las tablas de reporte."""
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

    # Cargar tabla maestra de zonas para validación referencial R06
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


def run_quality_gate_for_month(
    con: duckdb.DuckDBPyConnection,
    period: MonthPeriod,
    standardized_view: str,
    zones_csv_path: Path,
    quarantine_base_dir: Path = QUARANTINE_DIR,
) -> Tuple[str, Dict[str, int]]:
    """
    Evalúa las 9 reglas de calidad sobre la vista estandarizada del mes.
    - Separa filas aceptadas (con array warning_flags) y filas rechazadas (a cuarentena).
    - Persiste la cuarentena en Parquet y en tabla DuckDB.
    - Verifica la invariante: leídas == aceptadas + cuarentena.
    Devuelve el nombre de la vista de filas aceptadas y el diccionario de conteos.
    """
    init_quality_tables(con, zones_csv_path)

    eval_view = f"v_evaluated_{period.year}_{period.month:02d}"
    accepted_view = f"v_accepted_{period.year}_{period.month:02d}"

    # Evaluación vectorizada en una sola pasada SQL
    con.execute(f"""
        CREATE OR REPLACE TEMP VIEW {eval_view} AS
        WITH valid_geo_zones AS (
            SELECT location_id
            FROM raw_taxi_zones
            WHERE location_id IS NOT NULL
              AND location_id NOT IN (264, 265)
              AND COALESCE(borough, 'Unknown') NOT IN ('Unknown', 'N/A')
        ),
        dedup_tagged AS (
            SELECT
                s.*,
                ROW_NUMBER() OVER (
                    PARTITION BY
                        s.vendor_id, s.tpep_pickup_datetime, s.tpep_dropoff_datetime,
                        s.passenger_count, s.trip_distance, s.ratecode_id,
                        s.store_and_fwd_flag, s.pu_location_id, s.do_location_id,
                        s.payment_type, s.fare_amount, s.extra, s.mta_tax,
                        s.tip_amount, s.tolls_amount, s.improvement_surcharge,
                        s.total_amount, s.congestion_surcharge, s.airport_fee,
                        s.cbd_congestion_fee
                    ORDER BY s.tpep_pickup_datetime
                ) AS _dup_row_num,
                EPOCH(s.tpep_dropoff_datetime - s.tpep_pickup_datetime) / 60.0 AS _dur_min
            FROM {standardized_view} s
        ),
        rule_flags AS (
            SELECT
                d.*,
                -- Reglas de RECHAZO (REJECT)
                COALESCE(
                    d.tpep_pickup_datetime IS NULL
                    OR EXTRACT('year' FROM d.tpep_pickup_datetime) != {period.year}
                    OR EXTRACT('month' FROM d.tpep_pickup_datetime) != {period.month},
                    TRUE
                ) AS r01_violated,

                COALESCE(
                    d.tpep_dropoff_datetime IS NULL
                    OR d.tpep_pickup_datetime IS NULL
                    OR d.tpep_dropoff_datetime <= d.tpep_pickup_datetime,
                    TRUE
                ) AS r02_violated,

                COALESCE(
                    d.trip_distance IS NULL
                    OR d.trip_distance <= 0.0
                    OR d.trip_distance > 200.0,
                    TRUE
                ) AS r03_violated,

                COALESCE(
                    d.total_amount IS NULL
                    OR d.total_amount < 0.0
                    OR d.fare_amount IS NULL
                    OR d.fare_amount < 0.0,
                    TRUE
                ) AS r04_violated,

                COALESCE(
                    d.pu_location_id IS NULL
                    OR d.do_location_id IS NULL
                    OR d.pu_location_id NOT IN (SELECT location_id FROM valid_geo_zones)
                    OR d.do_location_id NOT IN (SELECT location_id FROM valid_geo_zones),
                    TRUE
                ) AS r06_violated,

                (d._dup_row_num > 1) AS r07_violated,

                COALESCE(
                    d._dur_min > 720.0
                    OR (
                        d._dur_min > 0
                        AND (d.trip_distance / (d._dur_min / 60.0)) > 100.0
                    ),
                    FALSE
                ) AS r08_violated,

                -- Reglas de ADVERTENCIA (WARNING)
                COALESCE(d.passenger_count IS NULL OR d.passenger_count <= 0, TRUE) AS r05_warning,
                COALESCE(d.passenger_count > 6, FALSE) AS r09_warning
            FROM dedup_tagged d
        )
        SELECT
            vendor_id, tpep_pickup_datetime, tpep_dropoff_datetime,
            passenger_count, trip_distance, ratecode_id, store_and_fwd_flag,
            pu_location_id, do_location_id, payment_type, fare_amount,
            extra, mta_tax, tip_amount, tolls_amount, improvement_surcharge,
            total_amount, congestion_surcharge, airport_fee, cbd_congestion_fee,
            r01_violated, r02_violated, r03_violated, r04_violated,
            r05_warning, r06_violated, r07_violated, r08_violated, r09_warning,
            LIST_FILTER(
                [
                    CASE WHEN r01_violated THEN 'R01_OUT_OF_MONTH_PICKUP' END,
                    CASE WHEN r02_violated THEN 'R02_DROPOFF_BEFORE_OR_EQUAL_PICKUP' END,
                    CASE WHEN r03_violated THEN 'R03_INVALID_TRIP_DISTANCE' END,
                    CASE WHEN r04_violated THEN 'R04_NEGATIVE_TOTAL_OR_FARE' END,
                    CASE WHEN r06_violated THEN 'R06_UNKNOWN_TAXI_ZONE' END,
                    CASE WHEN r07_violated THEN 'R07_EXACT_DUPLICATE_ROW' END,
                    CASE WHEN r08_violated THEN 'R08_IMPOSSIBLE_DURATION_OR_SPEED' END
                ],
                x -> x IS NOT NULL
            ) AS violated_reject_rules,
            LIST_FILTER(
                [
                    CASE WHEN r05_warning THEN 'R05_NULL_OR_ZERO_PASSENGERS' END,
                    CASE WHEN r09_warning THEN 'R09_EXCESSIVE_PASSENGER_COUNT' END
                ],
                x -> x IS NOT NULL
            ) AS warning_flags
        FROM rule_flags;
    """)

    # Materializar cuarentena del mes en Parquet y en tabla DuckDB
    quarantine_dir = (
        quar