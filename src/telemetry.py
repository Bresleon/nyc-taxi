import os
import threading
import time
from contextlib import contextmanager
from dataclasses import dataclass, field
from datetime import datetime, timezone
from pathlib import Path
from typing import Dict, Generator, List, Optional

import duckdb
import psutil

from src.config import PROJECT_ROOT, WAREHOUSE_PATH, get_duckdb_connection

RAM_BUDGET_MB = 8192.0  # Techo de 8 GB exigido por la cátedra


@dataclass
class StageMetricRecord:
    run_id: str
    period: str
    stage_name: str
    started_at: datetime
    duration_sec: float = 0.0
    ram_start_mb: float = 0.0
    ram_peak_mb: float = 0.0
    ram_avg_mb: float = 0.0
    ram_pct_of_8gb: float = 0.0
    cpu_user_sec: float = 0.0
    cpu_sys_sec: float = 0.0
    temp_spill_mb: float = 0.0
    rows_processed: int = 0
    throughput_rows_per_sec: float = 0.0
    samples: List[float] = field(default_factory=list, repr=False)


def _get_dir_size_mb(path: Path) -> float:
    if not path.exists():
        return 0.0
    total_bytes = sum(f.stat().st_size for f in path.rglob("*") if f.is_file())
    return round(total_bytes / (1024 * 1024), 2)


def init_metrics_table(con: duckdb.DuckDBPyConnection) -> None:
    con.execute("""
        CREATE TABLE IF NOT EXISTS main.pipeline_metrics_log (
            run_id VARCHAR,
            period VARCHAR,
            stage_name VARCHAR,
            started_at TIMESTAMP,
            duration_sec DOUBLE,
            ram_start_mb DOUBLE,
            ram_peak_mb DOUBLE,
            ram_avg_mb DOUBLE,
            ram_pct_of_8gb DOUBLE,
            cpu_user_sec DOUBLE,
            cpu_sys_sec DOUBLE,
            temp_spill_mb DOUBLE,
            rows_processed BIGINT,
            throughput_rows_per_sec DOUBLE,
            PRIMARY KEY (run_id, period, stage_name)
        );
    """)


@contextmanager
def track_stage_resources(
    run_id: str,
    stage_name: str,
    period: str = "ALL",
    rows_processed_ref: Optional[Dict[str, int]] = None,
    warehouse_path: Path = WAREHOUSE_PATH,
) -> Generator[StageMetricRecord, None, None]:
    """
    Muestrea memoria RAM (RSS), CPU y spilling a disco cada 250ms en un hilo daemon
    mientras se ejecuta una etapa del pipeline, y persiste la métrica en DuckDB.
    """
    proc = psutil.Process(os.getpid())
    tmp_dir = warehouse_path.parent / "duckdb_tmp"

    rss_init = proc.memory_info().rss / (1024 * 1024)
    cpu_init = proc.cpu_times()
    started_at = datetime.now(timezone.utc).replace(tzinfo=None)

    record = StageMetricRecord(
        run_id=run_id,
        period=period,
        stage_name=stage_name,
        started_at=started_at,
        ram_start_mb=round(rss_init, 2),
        ram_peak_mb=round(rss_init, 2),
        samples=[rss_init],
    )

    stop_event = threading.Event()

    def _sampler() -> None:
        while not stop_event.is_set():
            try:
                current_rss = proc.memory_info().rss / (1024 * 1024)
                record.samples.append(current_rss)
                if current_rss > record.ram_peak_mb:
                    record.ram_peak_mb = round(current_rss, 2)
            except psutil.Error:
                pass
            stop_event.wait(0.25)

    sampler_thread = threading.Thread(target=_sampler, daemon=True)
    t0 = time.perf_counter()
    sampler_thread.start()

    try:
        yield record
    finally:
        stop_event.set()
        sampler_thread.join(timeout=1.0)

        elapsed = max(0.001, time.perf_counter() - t0)
        cpu_end = proc.cpu_times()

        record.duration_sec = round(elapsed, 3)
        record.ram_avg_mb = round(sum(record.samples) / len(record.samples), 2)
        record.ram_pct_of_8gb = round((record.ram_peak_mb / RAM_BUDGET_MB) * 100.0, 2)
        record.cpu_user_sec = round(max(0.0, cpu_end.user - cpu_init.user), 3)
        record.cpu_sys_sec = round(max(0.0, cpu_end.system - cpu_init.system), 3)
        record.temp_spill_mb = _get_dir_size_mb(tmp_dir)

        if rows_processed_ref and "count" in rows_processed_ref:
            record.rows_processed = int(rows_processed_ref["count"])
        if record.rows_processed > 0:
            record.throughput_rows_per_sec = round(record.rows_processed / elapsed, 1)

        # Persistir en DuckDB si el archivo no está bloqueado por otro subproceso
        try:
            con = get_duckdb_connection(warehouse_path)
            try:
                init_metrics_table(con)
                con.execute(
                    """
                    INSERT OR REPLACE INTO main.pipeline_metrics_log VALUES
                    (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?);
                    """,
                    [
                        record.run_id,
                        record.period,
                        record.stage_name,
                        record.started_at,
                        record.duration_sec,
                        record.ram_start_mb,
                        record.ram_peak_mb,
                        record.ram_avg_mb,
                        record.ram_pct_of_8gb,
                        record.cpu_user_sec,
                        record.cpu_sys_sec,
                        record.temp_spill_mb,
                        record.rows_processed,
                        record.throughput_rows_per_sec,
                    ],
                )
            finally:
                con.close()
        except Exception as exc:
            print(
                f"[TELEMETRY WARN] No se pudo persistir métrica de '{stage_name}': {exc}"
            )
