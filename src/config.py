import os
import re
from dataclasses import dataclass
from pathlib import Path
from typing import List, Tuple

import duckdb
import psutil

PROJECT_ROOT = Path(__file__).resolve().parent.parent
DATA_DIR = PROJECT_ROOT / "data"
RAW_DIR = DATA_DIR / "raw"
QUARANTINE_DIR = DATA_DIR / "quarantine"
CURATED_DIR = DATA_DIR / "curated" / "yellow"
WAREHOUSE_PATH = DATA_DIR / "warehouse" / "nyc_taxi.duckdb"
CONTRACT_PATH = PROJECT_ROOT / "contracts" / "yellow_schema.yaml"
SQL_DIR = PROJECT_ROOT / "sql"
RESULTADOS_PATH = PROJECT_ROOT / "RESULTADOS.md"

TLC_TRIP_URL_TEMPLATE = "https://d37ci6vzurychx.cloudfront.net/trip-data/yellow_tripdata_{year}-{month:02d}.parquet"
TLC_ZONE_LOOKUP_URL = "https://d37ci6vzurychx.cloudfront.net/misc/taxi_zone_lookup.csv"


@dataclass(frozen=True)
class MonthPeriod:
    year: int
    month: int

    @property
    def label(self) -> str:
        return f"{self.year}-{self.month:02d}"

    @property
    def file_name(self) -> str:
        return f"yellow_tripdata_{self.year}-{self.month:02d}.parquet"


def parse_target_and_comparison_months(mes_str: str) -> List[MonthPeriod]:
    """
    Dado un mes 'AAAA-MM' (ej. '2025-03'), devuelve [(2024, 3), (2025, 3)]
    ordenados cronológicamente para procesar el mes asignado y el mismo mes de 2024.
    """
    match = re.match(r"^(\d{4})-(0[1-9]|1[0-2])$", mes_str.strip())
    if not match:
        raise ValueError(
            f"Formato de mes inválido: '{mes_str}'. Debe ser AAAA-MM (ej. 2025-03)."
        )
    year = int(match.group(1))
    month = int(match.group(2))

    comparison_year = 2024 if year != 2024 else 2023
    periods = sorted(
        {
            MonthPeriod(year=comparison_year, month=month),
            MonthPeriod(year=year, month=month),
        },
        key=lambda p: (p.year, p.month),
    )
    return periods


def get_duckdb_connection(db_path: Path = WAREHOUSE_PATH) -> duckdb.DuckDBPyConnection:
    """
    Crea y configura una conexión a DuckDB restringiendo el uso de memoria RAM
    a un máximo de 4GB y habilitando spilling a disco para cumplir con holgura
    la restricción de correr en una notebook de 8 GB de RAM.
    """
    db_path.parent.mkdir(parents=True, exist_ok=True)
    tmp_dir = db_path.parent / "duckdb_tmp"
    tmp_dir.mkdir(parents=True, exist_ok=True)

    con = duckdb.connect(str(db_path))
    con.execute("SET memory_limit = '4GB';")
    con.execute(f"SET temp_directory = '{tmp_dir.as_posix()}';")
    con.execute("SET preserve_insertion_order = false;")
    return con


def get_memory_usage_mb() -> float:
    """Devuelve la memoria física RSS actual del proceso en Megabytes."""
    process = psutil.Process(os.getpid())
    return process.memory_info().rss / (1024 * 1024)
