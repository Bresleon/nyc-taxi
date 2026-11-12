"""
Tests del contrato de esquema (paso 2).

Se arman Parquets de 1 viaje con los nombres y tipos ORIGINALES de la TLC
(VendorID INT32, PULocationID, Airport_fee...) y se verifica que el paso 2:
- normalice a snake_case,
- complete cbd_congestion_fee con 0.0 cuando no existe (2024) o viene nulo,
- avise e ignore columnas nuevas no contratadas,
- corte el pipeline si falta una columna obligatoria.
"""

from datetime import datetime
from pathlib import Path
from typing import Optional

import duckdb
import pyarrow as pa
import pyarrow.parquet as pq
import pytest

from src.config import MonthPeriod
from src.step2_schema import to_snake_case, validate_and_standardize_month_schema
from src.step3_quality import run_quality_gate_for_month

PERIOD_2024 = MonthPeriod(year=2024, month=3)


def _create_raw_tlc_parquet(
    file_path: Path,
    with_cbd: bool,
    new_column: bool = False,
    drop_column: Optional[str] = None,
) -> Path:
    """Parquet de 1 viaje válido con el esquema físico original de la TLC."""
    cols = {
        "VendorID": pa.array([2], pa.int32()),
        "tpep_pickup_datetime": pa.array([datetime(2024, 3, 5, 8, 0)], pa.timestamp("us")),
        "tpep_dropoff_datetime": pa.array([datetime(2024, 3, 5, 8, 20)], pa.timestamp("us")),
        "passenger_count": pa.array([1], pa.int64()),
        "trip_distance": [3.1],
        "RatecodeID": pa.array([1], pa.int64()),
        "store_and_fwd_flag": ["N"],
        "PULocationID": pa.array([161], pa.int32()),
        "DOLocationID": pa.array([237], pa.int32()),
        "payment_type": pa.array([1], pa.int64()),
        "fare_amount": [17.0],
        "extra": [0.0],
        "mta_tax": [0.5],
        "tip_amount": [3.0],
        "tolls_amount": [0.0],
        "improvement_surcharge": [1.0],
        "total_amount": [24.0],
        "congestion_surcharge": [2.5],
        "Airport_fee": [0.0],
    }
    if with_cbd:
        cols["cbd_congestion_fee"] = pa.array([None], pa.float64())
    if new_column:
        cols["ev_surcharge"] = [1.5]
    if drop_column:
        cols.pop(drop_column)
    pq.write_table(pa.table(cols), file_path)
    return file_path


def test_snake_case_normalization():
    assert to_snake_case("PULocationID") == "pu_location_id"
    assert to_snake_case("DOLocationID") == "do_location_id"
    assert to_snake_case("VendorID") == "vendor_id"
    assert to_snake_case("Airport_fee") == "airport_fee"
    assert to_snake_case("SomeNewColumn") == "some_new_column"


def test_missing_cbd_fee_in_2024_is_filled_with_zero(
    mem_con: duckdb.DuckDBPyConnection, tmp_path: Path
):
    raw = _create_raw_tlc_parquet(tmp_path / "raw.parquet", with_cbd=False)

    report, view = validate_and_standardize_month_schema(mem_con, PERIOD_2024, raw)

    assert report.missing_optional_columns == ["cbd_congestion_fee"]
    assert mem_con.execute(f"SELECT cbd_congestion_fee FROM {view};").fetchone()[0] == 0.0
    columns = [r[0] for r in mem_con.execute(f"DESCRIBE {view};").fetchall()]
    assert {"vendor_id", "pu_location_id", "do_location_id", "airport_fee"} <= set(columns)


def test_null_cbd_fee_is_coalesced_to_zero(mem_con: duckdb.DuckDBPyConnection, tmp_path: Path):
    raw = _create_raw_tlc_parquet(tmp_path / "raw.parquet", with_cbd=True)
    _, view = validate_and_standardize_month_schema(mem_con, PERIOD_2024, raw)
    assert mem_con.execute(f"SELECT cbd_congestion_fee FROM {view};").fetchone()[0] == 0.0


def test_new_column_is_reported_and_ignored(mem_con: duckdb.DuckDBPyConnection, tmp_path: Path):
    raw = _create_raw_tlc_parquet(tmp_path / "raw.parquet", with_cbd=True, new_column=True)

    report, view = validate_and_standardize_month_schema(mem_con, PERIOD_2024, raw)

    assert any("ev_surcharge" in c for c in report.new_unexpected_columns)
    columns = [r[0] for r in mem_con.execute(f"DESCRIBE {view};").fetchall()]
    assert "ev_surcharge" not in columns
    audit = mem_con.execute("SELECT new_columns FROM schema_audit_log;").fetchone()[0]
    assert "ev_surcharge" in audit


def test_missing_required_column_stops_pipeline(
    mem_con: duckdb.DuckDBPyConnection, tmp_path: Path
):
    raw = _create_raw_tlc_parquet(
        tmp_path / "raw.parquet", with_cbd=True, drop_column="trip_distance"
    )
    with pytest.raises(ValueError):
        validate_and_standardize_month_schema(mem_con, PERIOD_2024, raw)


def test_schema_then_quality_end_to_end(
    mem_con: duckdb.DuckDBPyConnection, synthetic_zones_csv: Path, tmp_path: Path
):
    """Archivo crudo de 2024 -> paso 2 -> paso 3: el viaje válido queda aceptado."""
    raw = _create_raw_tlc_parquet(tmp_path / "raw.parquet", with_cbd=False)
    _, view = validate_and_standardize_month_schema(mem_con, PERIOD_2024, raw)
    _, metrics = run_quality_gate_for_month(
        mem_con, PERIOD_2024, view, synthetic_zones_csv, tmp_path / "quarantine"
    )
    assert metrics["rows_accepted"] == 1
    assert metrics["rows_quarantined"] == 0
