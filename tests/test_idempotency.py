from pathlib import Path
from unittest.mock import patch

import duckdb

from src.config import MonthPeriod
from src.step1_ingest import ingest_month_parquet, init_ingestion_log
from tests.test_quality_rules import _create_synthetic_parquet


def test_ingestion_idempotency(mem_con: duckdb.DuckDBPyConnection, tmp_path: Path):
    init_ingestion_log(mem_con)
    period = MonthPeriod(year=2025, month=3)
    raw_dir = tmp_path / "raw"

    dest_dir = raw_dir / "yellow" / f"year={period.year}" / f"month={period.month:02d}"
    dest_path = dest_dir / period.file_name

    def fake_download(
        url: str, dest: Path, max_retries: int = 3, base_wait: float = 2.0
    ):
        dest.parent.mkdir(parents=True, exist_ok=True)
        _create_synthetic_parquet(dest, year=2025, month=3)

    # 1. Primera corrida: debe simular la descarga física y registrar en ingestion_log
    with patch(
        "src.step1_ingest.download_file_with_backoff", side_effect=fake_download
    ) as mock_dl:
        res1 = ingest_month_parquet(mem_con, period, raw_base_dir=raw_dir)
        assert res1.exists()
        assert mock_dl.call_count == 1

    log_count_1 = mem_con.execute(
        "SELECT COUNT(*) FROM ingestion_log WHERE file_name = ? AND status = 'ok'",
        [period.file_name],
    ).fetchone()[0]
    assert log_count_1 == 1

    # 2. Segunda corrida: archivo ya existe con el mismo hash -> no debe llamar a download_file_with_backoff
    with patch(
        "src.step1_ingest.download_file_with_backoff", side_effect=fake_download
    ) as mock_dl_2:
        res2 = ingest_month_parquet(mem_con, period, raw_base_dir=raw_dir)
        assert res2 == res1
        # No se produce tráfico de red en la segunda llamada
        assert mock_dl_2.call_count == 0

    log_count_2 = mem_con.execute(
        "SELECT COUNT(*) FROM ingestion_log WHERE file_name = ? AND status = 'ok'",
        [period.file_name],
    ).fetchone()[0]
    # No se crean filas duplicadas en el registro de control
    assert log_count_2 == 1
