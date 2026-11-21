from pathlib import Path
from unittest.mock import MagicMock

import pyarrow as pa
import pyarrow.parquet as pq
import pytest

import src.step1_ingest as step1_ingest
from src.config import MonthPeriod, get_duckdb_connection
from src.step1_ingest import run_dlt_ingestion


def _write_fake_zones_csv(dest_path: Path) -> None:
    dest_path.parent.mkdir(parents=True, exist_ok=True)
    dest_path.write_text(
        "LocationID,Borough,Zone,service_zone\n"
        "1,EWR,Newark Airport,EWR\n"
        "132,Queens,JFK Airport,Airports\n"
        "161,Manhattan,Midtown Center,Yellow Zone\n",
        encoding="utf-8",
    )


def _write_fake_trips_parquet(dest_path: Path, num_rows: int = 5) -> None:
    dest_path.parent.mkdir(parents=True, exist_ok=True)
    table = pa.table(
        {
            "VendorID": pa.array([1] * num_rows, type=pa.int64()),
            "PULocationID": pa.array([161] * num_rows, type=pa.int64()),
            "DOLocationID": pa.array([132] * num_rows, type=pa.int64()),
            "passenger_count": pa.array([1.0] * num_rows, type=pa.float64()),
            "trip_distance": pa.array([12.5] * num_rows, type=pa.float64()),
            "fare_amount": pa.array([45.0] * num_rows, type=pa.float64()),
            "total_amount": pa.array([52.5] * num_rows, type=pa.float64()),
        }
    )
    pq.write_table(table, dest_path)


def test_dlt_ingestion_is_idempotent(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    """
    Verifica que ejecutar run_dlt_ingestion dos veces para los mismos períodos:
    1. No vuelve a descargar archivos si ya existen en disco y su SHA-256 coincide.
    2. No duplica registros en main.ingestion_log ni en raw_dlt.yellow_trips_raw.
    3. Registra paquetes de carga exitosos en raw_dlt._dlt_loads.
    """
    raw_dir = tmp_path / "raw"
    warehouse_path = tmp_path / "warehouse" / "test_nyc_taxi.duckdb"
    dlt_state_dir = tmp_path / "dlt_state"
    monkeypatch.setattr(step1_ingest, "DLT_PIPELINES_DIR", dlt_state_dir)

    periods = [
        MonthPeriod(year=2024, month=3),
        MonthPeriod(year=2025, month=3),
    ]

    download_mock = MagicMock()

    def fake_download(
        url: str, dest_path: Path, max_retries: int = 3, base_wait: float = 2.0
    ) -> None:
        download_mock(url, dest_path)
        if dest_path.name.endswith(".csv"):
            _write_fake_zones_csv(dest_path)
        elif "2024-03" in dest_path.name:
            _write_fake_trips_parquet(dest_path, num_rows=4)
        else:
            _write_fake_trips_parquet(dest_path, num_rows=6)

    monkeypatch.setattr(step1_ingest, "download_file_with_backoff", fake_download)

    # Primera ejecución: debe descargar 1 CSV + 2 Parquet = 3 llamadas de red
    first_result = run_dlt_ingestion(
        periods=periods,
        raw_base_dir=raw_dir,
        warehouse_path=warehouse_path,
    )
    assert download_mock.call_count == 3
    assert first_result["total_rows_logged"] == 10

    # Segunda ejecución: idempotente -> 0 descargas nuevas
    second_result = run_dlt_ingestion(
        periods=periods,
        raw_base_dir=raw_dir,
        warehouse_path=warehouse_path,
    )
    assert download_mock.call_count == 3
    assert second_result["total_rows_logged"] == 10

    con = get_duckdb_connection(warehouse_path)
    try:
        # Exactamente 3 filas en main.ingestion_log (1 por archivo, sin duplicados)
        log_rows = con.execute(
            "SELECT file_name, row_count, status, error_message FROM main.ingestion_log ORDER BY file_name;"
        ).fetchall()
        assert len(log_rows) == 3
        assert all(row[2] == "ok" and row[3] is None for row in log_rows)

        # Exactamente 2 paquetes de carga exitosos (status = 0) en la tabla nativa de dlt
        dlt_loads = con.execute(
            "SELECT COUNT(*) FROM raw_dlt._dlt_loads WHERE status = 0;"
        ).fetchone()[0]
        assert dlt_loads == 2

        # Sin duplicación de filas en raw_dlt.yellow_trips_raw
        raw_trips_count = con.execute(
            "SELECT COUNT(*) FROM raw_dlt.yellow_trips_raw;"
        ).fetchone()[0]
        assert raw_trips_count == 10
    finally:
        con.close()


def test_dlt_ingestion_redownloads_on_hash_mismatch(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    """
    Si un archivo existente en disco fue alterado o truncado (SHA-256 distinto al registrado),
    la ingesta vuelve a descargarlo y actualiza su registro de auditoría.
    """
    raw_dir = tmp_path / "raw"
    warehouse_path = tmp_path / "warehouse" / "test_hash.duckdb"
    monkeypatch.setattr(step1_ingest, "DLT_PIPELINES_DIR", tmp_path / "dlt_state")

    periods = [MonthPeriod(year=2025, month=3)]
    download_calls = {"count": 0}

    def fake_download(
        url: str, dest_path: Path, max_retries: int = 3, base_wait: float = 2.0
    ) -> None:
        download_calls["count"] += 1
        if dest_path.name.endswith(".csv"):
            _write_fake_zones_csv(dest_path)
        else:
            _write_fake_trips_parquet(dest_path, num_rows=5)

    monkeypatch.setattr(step1_ingest, "download_file_with_backoff", fake_download)

    run_dlt_ingestion(
        periods=periods, raw_base_dir=raw_dir, warehouse_path=warehouse_path
    )
    assert download_calls["count"] == 2  # 1 CSV + 1 Parquet

    # Corromper el archivo Parquet en disco para alterar su hash SHA-256
    parquet_file = (
        raw_dir
        / "yellow"
        / "year=2025"
        / "month=03"
        / "yellow_tripdata_2025-03.parquet"
    )
    parquet_file.write_bytes(b"corrupted_parquet_bytes")

    run_dlt_ingestion(
        periods=periods, raw_base_dir=raw_dir, warehouse_path=warehouse_path
    )
    assert download_calls["count"] == 3  # Re-descargó únicamente el Parquet alterado


def test_dlt_ingestion_exits_nonzero_on_network_failure(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    """
    Ante un fallo definitivo de red tras agotar los reintentos, run_dlt_ingestion
    termina con código de salida distinto de cero (SystemExit 1).
    """
    monkeypatch.setattr(step1_ingest, "DLT_PIPELINES_DIR", tmp_path / "dlt_state")

    def failing_download(
        url: str, dest_path: Path, max_retries: int = 3, base_wait: float = 2.0
    ) -> None:
        raise RuntimeError("Fallo simulado de red tras 3 reintentos")

    monkeypatch.setattr(step1_ingest, "download_file_with_backoff", failing_download)

    with pytest.raises(SystemExit) as exc_info:
        run_dlt_ingestion(
            periods=[MonthPeriod(year=2025, month=3)],
            raw_base_dir=tmp_path / "raw",
            warehouse_path=tmp_path / "warehouse" / "fail.duckdb",
        )

    assert exc_info.value.code == 1
