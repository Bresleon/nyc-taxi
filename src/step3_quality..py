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
        quarantine_base_dir / f"year={period.year}" / f"month={period.month:02d}"
    )
    quarantine_dir.mkdir(parents=True, exist_ok=True)
    quarantine_file = quarantine_dir / f"quarantine_{period.label}.parquet"
    if quarantine_file.exists():
        quarantine_file.unlink()

    con.execute(f"""
        COPY (
            SELECT
                '{period.file_name}' AS source_file,
                '{period.label}' AS period,
                vendor_id, tpep_pickup_datetime, tpep_dropoff_datetime,
                passenger_count, trip_distance, ratecode_id, store_and_fwd_flag,
                pu_location_id, do_location_id, payment_type, fare_amount,
                extra, mta_tax, tip_amount, tolls_amount, improvement_surcharge,
                total_amount, congestion_surcharge, airport_fee, cbd_congestion_fee,
                violated_reject_rules,
                warning_flags
            FROM {eval_view}
            WHERE len(violated_reject_rules) > 0
        ) TO '{quarantine_file.as_posix()}' (FORMAT PARQUET, COMPRESSION ZSTD);
    """)

    # Registrar también en tabla SQL quarantine_trips para consulta directa en la defensa oral
    con.execute(
        """
        CREATE TABLE IF NOT EXISTS quarantine_trips AS
        SELECT * FROM read_parquet(?) WHERE 1=0;
        """,
        [quarantine_file.as_posix()],
    )
    con.execute("DELETE FROM quarantine_trips WHERE period = ?;", [period.label])
    con.execute(
        "INSERT INTO quarantine_trips SELECT * FROM read_parquet(?);",
        [quarantine_file.as_posix()],
    )

    # Vista de filas aceptadas (sin violaciones de rechazo)
    con.execute(f"""
        CREATE OR REPLACE TEMP VIEW {accepted_view} AS
        SELECT
            vendor_id, tpep_pickup_datetime, tpep_dropoff_datetime,
            passenger_count, trip_distance, ratecode_id, store_and_fwd_flag,
            pu_location_id, do_location_id, payment_type, fare_amount,
            extra, mta_tax, tip_amount, tolls_amount, improvement_surcharge,
            total_amount, congestion_surcharge, airport_fee, cbd_congestion_fee,
            warning_flags
        FROM {eval_view}
        WHERE len(violated_reject_rules) = 0;
    """)

    # Conteos y reconciliación estricta
    counts_row = con.execute(f"""
        SELECT
            (SELECT COUNT(*) FROM {standardized_view}) AS rows_read,
            (SELECT COUNT(*) FROM {accepted_view}) AS rows_accepted,
            (SELECT COUNT(*) FROM read_parquet('{quarantine_file.as_posix()}')) AS rows_quarantined,
            (SELECT COUNT(*) FROM {accepted_view} WHERE len(warning_flags) > 0) AS rows_with_warnings,
            SUM(CASE WHEN r01_violated THEN 1 ELSE 0 END) AS r01,
            SUM(CASE WHEN r02_violated THEN 1 ELSE 0 END) AS r02,
            SUM(CASE WHEN r03_violated THEN 1 ELSE 0 END) AS r03,
            SUM(CASE WHEN r04_violated THEN 1 ELSE 0 END) AS r04,
            SUM(CASE WHEN r05_warning  THEN 1 ELSE 0 END) AS r05,
            SUM(CASE WHEN r06_violated THEN 1 ELSE 0 END) AS r06,
            SUM(CASE WHEN r07_violated THEN 1 ELSE 0 END) AS r07,
            SUM(CASE WHEN r08_violated THEN 1 ELSE 0 END) AS r08,
            SUM(CASE WHEN r09_warning  THEN 1 ELSE 0 END) AS r09
        FROM {eval_view};
    """).fetchone()

    rows_read = int(counts_row[0] or 0)
    rows_accepted = int(counts_row[1] or 0)
    rows_quarantined = int(counts_row[2] or 0)
    rows_with_warnings = int(counts_row[3] or 0)

    rule_breakdown = {
        "R01_OUT_OF_MONTH_PICKUP": int(counts_row[4] or 0),
        "R02_DROPOFF_BEFORE_OR_EQUAL_PICKUP": int(counts_row[5] or 0),
        "R03_INVALID_TRIP_DISTANCE": int(counts_row[6] or 0),
        "R04_NEGATIVE_TOTAL_OR_FARE": int(counts_row[7] or 0),
        "R05_NULL_OR_ZERO_PASSENGERS": int(counts_row[8] or 0),
        "R06_UNKNOWN_TAXI_ZONE": int(counts_row[9] or 0),
        "R07_EXACT_DUPLICATE_ROW": int(counts_row[10] or 0),
        "R08_IMPOSSIBLE_DURATION_OR_SPEED": int(counts_row[11] or 0),
        "R09_EXCESSIVE_PASSENGER_COUNT": int(counts_row[12] or 0),
    }

    reconcile_records(
        period_label=period.label,
        rows_read=rows_read,
        rows_accepted=rows_accepted,
        rows_quarantined=rows_quarantined,
    )

    now_utc = datetime.now(timezone.utc).replace(tzinfo=None)
    con.execute(
        """
        INSERT OR REPLACE INTO quality_summary_report
        VALUES (?, ?, ?, ?, ?, ?, ?, ?);
        """,
        [
            period.label,
            period.file_name,
            rows_read,
            rows_accepted,
            rows_quarantined,
            rows_with_warnings,
            True,
            now_utc,
        ],
    )

    for r in QUALITY_RULES:
        c = rule_breakdown[r.rule_id]
        pct = round((c * 100.0) / rows_read, 4) if rows_read > 0 else 0.0
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


def reconcile_records(
    period_label: str, rows_read: int, rows_accepted: int, rows_quarantined: int
) -> None:
    """
    Invariante obligatoria del pipeline:
    filas_leidas == filas_aceptadas + filas_cuarentena.
    Si no se cumple, lanza RuntimeError deteniendo la ejecución.
    """
    if rows_read != (rows_accepted + rows_quarantined):
        raise RuntimeError(
            f"[ERROR DE RECONCILIACIÓN en {period_label}] "
            f"filas_leídas ({rows_read}) != filas_aceptadas ({rows_accepted}) + "
            f"filas_cuarentena ({rows_quarantined}). Diferencia: "
            f"{rows_read - (rows_accepted + rows_quarantined)}"
        )
