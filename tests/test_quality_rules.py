"""
Test de reglas de calidad sobre datos sintéticos (paso 3).

Se arma un Parquet con 9 viajes de marzo 2025, cada uno pensado para disparar
una regla, y se verifica que:
- las filas con reglas REJECT terminen en cuarentena con la regla correcta,
- la fila con WARNING (0 pasajeros) quede aceptada con su flag,
- leídas == aceptadas + cuarentena,
- correr dos veces no duplique la cuarentena.
"""

from datetime import datetime
from pathlib import Path

import duckdb
import pyarrow as pa
import pyarrow.parquet as pq

from src.config import MonthPeriod
from src.step3_quality import run_quality_gate_for_month


def _create_synthetic_parquet(file_path: Path, year: int = 2025, month: int = 3) -> Path:
    """Genera un Parquet sintético (ya en snake_case) con casos borde diseñados."""
    schema = pa.schema([
        ("vendor_id", pa.int64()),
        ("tpep_pickup_datetime", pa.timestamp("us")),
        ("tpep_dropoff_datetime", pa.timestamp("us")),
        ("passenger_count", pa.float64()),
        ("trip_distance", pa.float64()),
        ("ratecode_id", pa.float64()),
        ("store_and_fwd_flag", pa.string()),
        ("pu_location_id", pa.int64()),
        ("do_location_id", pa.int64()),
        ("payment_type", pa.int64()),
        ("fare_amount", pa.float64()),
        ("extra", pa.float64()),
        ("mta_tax", pa.float64()),
        ("tip_amount", pa.float64()),
        ("tolls_amount", pa.float64()),
        ("improvement_surcharge", pa.float64()),
        ("total_amount", pa.float64()),
        ("congestion_surcharge", pa.float64()),
        ("airport_fee", pa.float64()),
        ("cbd_congestion_fee", pa.float64()),
    ])

    d = datetime
    data = [
        # 1. Fila válida
        (1, d(2025, 3, 10, 10, 0), d(2025, 3, 10, 10, 15), 1.0, 2.5, 1.0, "N", 161, 237, 1, 15.0, 0.0, 0.5, 3.0, 0.0, 1.0, 20.25, 2.5, 0.0, 0.75),
        # 2. R01: fuera de mes (febrero en archivo de marzo) -> REJECT
        (1, d(2025, 2, 28, 23, 59), d(2025, 3, 1, 0, 10), 1.0, 1.5, 1.0, "N", 161, 237, 1, 10.0, 0.0, 0.5, 2.0, 0.0, 1.0, 14.25, 2.5, 0.0, 0.75),
        # 3. R02: bajada anterior a la subida -> REJECT
        (1, d(2025, 3, 10, 12, 0), d(2025, 3, 10, 11, 50), 1.0, 2.0, 1.0, "N", 161, 237, 1, 12.0, 0.0, 0.5, 2.0, 0.0, 1.0, 16.25, 2.5, 0.0, 0.75),
        # 4. R03: distancia <= 0 -> REJECT
        (1, d(2025, 3, 10, 14, 0), d(2025, 3, 10, 14, 20), 1.0, 0.0, 1.0, "N", 161, 237, 1, 15.0, 0.0, 0.5, 0.0, 0.0, 1.0, 19.0, 2.5, 0.0, 0.75),
        # 5. R04: importes negativos -> REJECT
        (1, d(2025, 3, 10, 15, 0), d(2025, 3, 10, 15, 15), 1.0, 3.0, 1.0, "N", 161, 237, 2, -15.0, 0.0, -0.5, 0.0, 0.0, -1.0, -16.5, 0.0, 0.0, 0.0),
        # 6. R05: 0 pasajeros -> WARNING (queda aceptada con flag)
        (1, d(2025, 3, 10, 16, 0), d(2025, 3, 10, 16, 20), 0.0, 4.0, 1.0, "N", 161, 237, 1, 18.0, 0.0, 0.5, 4.0, 0.0, 1.0, 24.25, 2.5, 0.0, 0.75),
        # 7. R06: zona de origen inexistente (999) -> REJECT
        (1, d(2025, 3, 10, 17, 0), d(2025, 3, 10, 17, 15), 1.0, 2.0, 1.0, "N", 999, 237, 1, 12.0, 0.0, 0.5, 2.0, 0.0, 1.0, 16.25, 2.5, 0.0, 0.75),
        # 8. R07: duplicado exacto de la fila 1 -> REJECT (segunda copia)
        (1, d(2025, 3, 10, 10, 0), d(2025, 3, 10, 10, 15), 1.0, 2.5, 1.0, "N", 161, 237, 1, 15.0, 0.0, 0.5, 3.0, 0.0, 1.0, 20.25, 2.5, 0.0, 0.75),
        # 9. R08: 30 millas en 2 minutos (900 mph) -> REJECT
        (1, d(2025, 3, 10, 18, 0), d(2025, 3, 10, 18, 2), 1.0, 30.0, 1.0, "N", 161, 237, 1, 50.0, 0.0, 0.5, 10.0, 0.0, 1.0, 64.75, 2.5, 0.0, 0.75),
    ]

    pydict = {field.name: [row[i] for row in data] for i, field in enumerate(schema)}
    pq.write_table(pa.Table.from_pydict(pydict, schema=schema), file_path)
    return file_path


def _standardized_view(con: duckdb.DuckDBPyConnection, tmp_path: Path) -> str:
    raw_parquet = tmp_path / "test_raw.parquet"
    _create_synthetic_parquet(raw_parquet)
    con.execute(
        f"CREATE OR REPLACE VIEW v_std_test AS SELECT * FROM read_parquet('{raw_parquet.as_posix()}');"
    )
    return "v_std_test"


def test_quality_rules_segregation(
    mem_con: duckdb.DuckDBPyConnection, synthetic_zones_csv: Path, tmp_path: Path
):
    view = _standardized_view(mem_con, tmp_path)
    quarantine_dir = tmp_path / "quarantine"

    accepted_view, metrics = run_quality_gate_for_month(
        con=mem_con,
        period=MonthPeriod(year=2025, month=3),
        standardized_view=view,
        zones_csv_path=synthetic_zones_csv,
        quarantine_base_dir=quarantine_dir,
    )

    # 9 leídas = 2 aceptadas (fila 1 limpia + fila 6 con warning) + 7 en cuarentena
    assert metrics["rows_read"] == 9
    assert metrics["rows_accepted"] == 2
    assert metrics["rows_quarantined"] == 7

    # La fila con passenger_count = 0 quedó aceptada con su flag R05
    warning_rows = mem_con.execute(f"""
        SELECT passenger_count, warning_flags
        FROM {accepted_view}
        WHERE len(warning_flags) > 0;
    """).fetchall()
    assert len(warning_rows) == 1
    assert warning_rows[0][0] == 0.0
    assert "R05_NULL_OR_ZERO_PASSENGERS" in warning_rows[0][1]

    # Cada regla de rechazo aparece en la cuarentena
    rules_detected = {
        r[0] for r in mem_con.execute(
            "SELECT unnest(violated_reject_rules) FROM quarantine_trips;"
        ).fetchall()
    }
    for rule in [
        "R01_OUT_OF_MONTH_PICKUP",
        "R02_DROPOFF_BEFORE_OR_EQUAL_PICKUP",
        "R03_INVALID_TRIP_DISTANCE",
        "R04_NEGATIVE_TOTAL_OR_FARE",
        "R06_UNKNOWN_TAXI_ZONE",
        "R07_EXACT_DUPLICATE_ROW",
        "R08_IMPOSSIBLE_DURATION_OR_SPEED",
    ]:
        assert rule in rules_detected

    # La cuarentena guarda el archivo de origen
    source_files = {
        r[0] for r in mem_con.execute("SELECT DISTINCT source_file FROM quarantine_trips;").fetchall()
    }
    assert source_files == {"yellow_tripdata_2025-03.parquet"}

    # Se escribió el Parquet de cuarentena en la carpeta del mes
    assert (quarantine_dir / "year=2025" / "month=03" / "quarantine_2025-03.parquet").exists()


def test_quality_gate_rerun_does_not_duplicate_quarantine(
    mem_con: duckdb.DuckDBPyConnection, synthetic_zones_csv: Path, tmp_path: Path
):
    """Correr dos veces el mismo mes deja los mismos conteos (demo del oral)."""
    view = _standardized_view(mem_con, tmp_path)
    for _ in range(2):
        _, metrics = run_quality_gate_for_month(
            mem_con, MonthPeriod(2025, 3), view, synthetic_zones_csv, tmp_path / "q"
        )
    assert metrics["rows_quarantined"] == 7
    assert mem_con.execute("SELECT COUNT(*) FROM quarantine_trips;").fetchone()[0] == 7
