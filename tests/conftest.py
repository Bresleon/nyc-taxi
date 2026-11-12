"""
Fixtures compartidos por todos los tests.
Conexión DuckDB en memoria y tabla de zonas sintética: los tests no tocan
data/ ni descargan nada de internet.
"""

from pathlib import Path
from typing import Generator

import duckdb
import pytest


@pytest.fixture
def mem_con() -> Generator[duckdb.DuckDBPyConnection, None, None]:
    """Conexión DuckDB en memoria, aislada para cada test."""
    con = duckdb.connect(":memory:")
    con.execute("SET preserve_insertion_order = false;")
    yield con
    con.close()


@pytest.fixture
def synthetic_zones_csv(tmp_path: Path) -> Path:
    """Tabla de zonas reducida (incluye 264/265 = Unknown)."""
    zones_file = tmp_path / "taxi_zone_lookup.csv"
    zones_file.write_text(
        "LocationID,Borough,Zone,service_zone\n"
        "1,EWR,Newark Airport,EWR\n"
        "2,Queens,Jamaica Bay,Boro Zone\n"
        "132,Queens,JFK Airport,Airports\n"
        "138,Queens,LaGuardia Airport,Airports\n"
        "161,Manhattan,Midtown Center,Yellow Zone\n"
        "237,Manhattan,Upper East Side South,Yellow Zone\n"
        "264,Unknown,NV,N/A\n"
        "265,Unknown,NA,N/A\n",
        encoding="utf-8",
    )
    return zones_file
