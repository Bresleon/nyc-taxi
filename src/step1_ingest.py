import hashlib
import os
import sys
import time
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Dict, Iterator, List, Optional

import dlt
import duckdb
import pyarrow.parquet as pq
import requests

from src.config import (
    DATA_DIR,
    RAW_DIR,
    TLC_TRIP_URL_TEMPLATE,
    TLC_ZONE_LOOKUP_URL,
    WAREHOUSE_PATH,
    MonthPeriod,
    get_duckdb_connection,
)

DLT_PIPELINES_DIR = DATA_DIR / "dlt_state"


def compute_sha256(file_path: Path) -> str:
    """Calcula el hash SHA-256 de un archivo en bloques de 64KB (O(1) RAM)."""
    sha256 = hashlib.sha256()
    with open(file_path, "rb") as f:
        for chunk in iter(lambda: f.read(65536), b""):
            sha256.update(chunk)
    return sha256.hexdigest()


def download_file_with_backoff(
    url: str, dest_path: Path, max_retries: int = 3, base_wait: float = 2.0
) -> None:
    """
    Descarga un archivo vía streaming a un archivo temporal (.tmp) y lo renombra
    atómicamente al finalizar. Reintenta hasta max_retries veces con espera creciente.
    """
    dest_path.parent.mkdir(parents=True, exist_ok=True)
    tmp_path = dest_path.with_suffix(dest_path.suffix + ".tmp")

    for attempt in range(1, max_retries + 1):
        try:
            with requests.get(url, stream=True, timeout=60) as response:
                response.raise_for_status()
                with open(tmp_path, "wb") as f:
                    for chunk in response.iter_content(chunk_size=65536):
                        if chunk:
                            f.write(chunk)
            os.replace(tmp_path, dest_path)
            return
        except Exception as exc:
            if tmp_path.exists():
                tmp_path.unlink()
            if attempt == max_retries:
                raise RuntimeError(
                    f"Fallo de red tras {max_retries} intentos descargando {url}: {exc}"
                ) from exc
            wait_sec = base_wait**attempt
            print(
                f"  [WARN] Intento {attempt}/{max_retries} falló ({exc}). "
                f"Reintentando en {wait_sec:.1f}s..."
            )
            time.sleep(wait_sec)


def _get_existing_logged_hash(
    warehouse_path: Path, file_name: str
) -> Optional[Dict[str, Any]]:
    """Consulta si el archivo ya fue ingestado con éxito y devuelve su hash y filas."""
    if not warehouse_path.exists():
        return None
    con = get_duckdb_connection(warehouse_path)
    try:
        exists = con.execute(
            """
            SELECT COUNT(*)
            FROM information_schema.tables
            WHERE table_schema = 'raw_dlt' AND table_name = 'ingestion_log';
            """
        ).fetchone()[0]
        if not exists:
            return None

        row = con.execute(
            """
            SELECT sha256, row_count, bytes
            FROM raw_dlt.ingestion_log
            WHERE file_name = ? AND status = 'ok'
            ORDER BY ingested_at DESC
            LIMIT 1;
            """,
            [file_name],
        ).fetchone()
        if row:
            return {"sha256": row[0], "row_count": row[1], "bytes": row[2]}
        return None
    finally:
        con.close()


@dlt.source(name="nyc_tlc_source")
def nyc_tlc_source(
    periods: List[MonthPeriod],
    raw_base_dir: Path = RAW_DIR,
    warehouse_path: Path = WAREHOUSE_PATH,
    max_retries: int = 3,
    base_wait: float = 2.0,
):
    """
    Fuente declarativa de dlt que:
    1. Descarga de forma idempotente los archivos Parquet y CSV a data/raw/ sin modificarlos.
    2. Registra el estado y trazabilidad en raw_dlt.ingestion_log (con estado 'ok' o 'error').
    3. Carga los viajes y zonas hacia el esquema raw_dlt en DuckDB mediante PyArrow batches
       (O(1) RAM), generando _dlt_loads y _dlt_version para auditoría nativa.
    """
    downloaded_parquets: List[tuple[MonthPeriod, Path, bool]] = []
    zones_path_holder: Dict[str, Any] = {}

    @dlt.resource(
        name="ingestion_log",
        write_disposition="merge",
        primary_key="file_name",
        columns={
            "file_name": {"data_type": "text", "nullable": False},
            "url": {"data_type": "text", "nullable": True},
            "row_count": {"data_type": "bigint", "nullable": True},
            "bytes": {"data_type": "bigint", "nullable": True},
            "sha256": {"data_type": "text", "nullable": True},
            "ingested_at": {"data_type": "timestamp", "nullable": True},
            "status": {"data_type": "text", "nullable": True},
            "error_message": {"data_type": "text", "nullable": True},
        },
    )
    def ingestion_log_resource() -> Iterator[Dict[str, Any]]:
        # 1. Descarga idempotente de tabla de zonas
        zones_file = "taxi_zone_lookup.csv"
        zones_dest = raw_base_dir / "zones" / zones_file
        zones_is_new = True

        if zones_dest.exists():
            disk_hash = compute_sha256(zones_dest)
            logged = _get_existing_logged_hash(warehouse_path, zones_file)
            if logged and logged["sha256"] == disk_hash:
                print(
                    f"[DLT INGESTA] Idempotente OK: '{zones_file}' ya existe "
                    f"({logged['row_count']} zonas). Omitiendo descarga."
                )
                zones_is_new = False
                zones_path_holder["path"] = zones_dest
                zones_path_holder["is_new"] = False
                yield {
                    "file_name": zones_file,
                    "url": TLC_ZONE_LOOKUP_URL,
                    "row_count": logged["row_count"],
                    "bytes": logged["bytes"],
                    "sha256": disk_hash,
                    "ingested_at": datetime.now(timezone.utc),
                    "status": "ok",
                    "error_message": None,
                }

        if zones_is_new:
            try:
                if not zones_dest.exists():
                    print(f"[DLT INGESTA] Descargando {TLC_ZONE_LOOKUP_URL} ...")
                    download_file_with_backoff(
                        TLC_ZONE_LOOKUP_URL, zones_dest, max_retries, base_wait
                    )
                disk_hash = compute_sha256(zones_dest)
                file_bytes = zones_dest.stat().st_size
                with open(zones_dest, "rb") as f:
                    row_count = max(0, sum(1 for _ in f) - 1)
                zones_path_holder["path"] = zones_dest
                zones_path_holder["is_new"] = True
                print(f"[DLT INGESTA] Completada '{zones_file}': {row_count} zonas.")
                yield {
                    "file_name": zones_file,
                    "url": TLC_ZONE_LOOKUP_URL,
                    "row_count": row_count,
                    "bytes": file_bytes,
                    "sha256": disk_hash,
                    "ingested_at": datetime.now(timezone.utc),
                    "status": "ok",
                    "error_message": None,
                }
            except Exception as exc:
                yield {
                    "file_name": zones_file,
                    "url": TLC_ZONE_LOOKUP_URL,
                    "row_count": 0,
                    "bytes": 0,
                    "sha256": None,
                    "ingested_at": datetime.now(timezone.utc),
                    "status": "error",
                    "error_message": str(exc),
                }
                raise

        # 2. Descarga idempotente de archivos Parquet mensuales
        for period in periods:
            url = TLC_TRIP_URL_TEMPLATE.format(year=period.year, month=period.month)
            dest_dir = (
                raw_base_dir
                / "yellow"
                / f"year={period.year}"
                / f"month={period.month:02d}"
            )
            dest_path = dest_dir / period.file_name
            needs_download = True

            if dest_path.exists():
                disk_hash = compute_sha256(dest_path)
                logged = _get_existing_logged_hash(warehouse_path, period.file_name)
                if logged and logged["sha256"] == disk_hash:
                    print(
                        f"[DLT INGESTA] Idempotente OK: '{period.file_name}' ya existe y su hash "
                        f"SHA-256 ({disk_hash[:12]}...) coincide ({logged['row_count']:,} filas)."
                    )
                    needs_download = False
                    downloaded_parquets.append((period, dest_path, False))
                    yield {
                        "file_name": period.file_name,
                        "url": url,
                        "row_count": logged["row_count"],
                        "bytes": logged["bytes"],
                        "sha256": disk_hash,
                        "ingested_at": datetime.now(timezone.utc),
                        "status": "ok",
                        "error_message": None,
                    }

            if needs_download:
                try:
                    if not dest_path.exists():
                        print(f"[DLT INGESTA] Descargando {url} -> {dest_path} ...")
                        download_file_with_backoff(
                            url, dest_path, max_retries, base_wait
                        )
                    disk_hash = compute_sha256(dest_path)
                    file_bytes = dest_path.stat().st_size
                    row_count = pq.read_metadata(dest_path).num_rows

                    downloaded_parquets.append((period, dest_path, True))
                    print(
                        f"[DLT INGESTA] Completada '{period.file_name}': {row_count:,} filas | "
                        f"{file_bytes / (1024 * 1024):.2f} MB | SHA-256: {disk_hash[:16]}..."
                    )
                    yield {
                        "file_name": period.file_name,
                        "url": url,
                        "row_count": row_count,
                        "bytes": file_bytes,
                        "sha256": disk_hash,
                        "ingested_at": datetime.now(timezone.utc),
                        "status": "ok",
                        "error_message": None,
                    }
                except Exception as exc:
                    yield {
                        "file_name": period.file_name,
                        "url": url,
                        "row_count": 0,
                        "bytes": 0,
                        "sha256": None,
                        "ingested_at": datetime.now(timezone.utc),
                        "status": "error",
                        "error_message": str(exc),
                    }
                    raise

    @dlt.resource(
        name="taxi_zones",
        write_disposition="replace",
    )
    def taxi_zones_resource():
        import pyarrow.csv as pv

        zones_dest: Optional[Path] = zones_path_holder.get("path")
        if zones_dest and zones_dest.exists():
            yield pv.read_csv(zones_dest)

    @dlt.resource(
        name="yellow_trips_raw",
        write_disposition="replace",
    )
    def yellow_trips_raw_resource():
        """
        Lee los archivos Parquet descargados en bloques columnares PyArrow de 100,000 filas
        para que dlt los cargue en DuckDB sin materializar diccionarios en RAM.
        """
        for period, parquet_path, _ in downloaded_parquets:
            pf = pq.ParquetFile(parquet_path)
            for batch in pf.iter_batches(batch_size=100_000):
                yield (
                    batch.to_pydict() if False else batch
                )  # Entrega RecordBatch nativo a dlt

    return [ingestion_log_resource, taxi_zones_resource, yellow_trips_raw_resource]


def run_dlt_ingestion(
    periods: List[MonthPeriod],
    raw_base_dir: Path = RAW_DIR,
    warehouse_path: Path = WAREHOUSE_PATH,
    max_retries: int = 3,
    base_wait: float = 2.0,
) -> Dict[str, Any]:
    """
    Ejecuta el pipeline de dlt hacia DuckDB (esquema 'raw_dlt'), registra trazabilidad
    nativa (_dlt_loads, _dlt_version) y expone una vista main.ingestion_log.
    Si ocurre un error de red definitivo, registra el fallo y termina con sys.exit(1).
    """
    DLT_PIPELINES_DIR.mkdir(parents=True, exist_ok=True)
    warehouse_path.parent.mkdir(parents=True, exist_ok=True)

    pipeline = dlt.pipeline(
        pipeline_name="nyc_taxi_ingestion",
        pipelines_dir=str(DLT_PIPELINES_DIR),
        destination=dlt.destinations.duckdb(str(warehouse_path)),
        dataset_name="raw_dlt",
    )

    source = nyc_tlc_source(
        periods=periods,
        raw_base_dir=raw_base_dir,
        warehouse_path=warehouse_path,
        max_retries=max_retries,
        base_wait=base_wait,
    )

    try:
        load_info = pipeline.run(source)
        # Verificar si algún paquete de carga reportó fallos internos en dlt
        load_info.raise_on_failed_jobs()

        # Crear vista de compatibilidad main.ingestion_log apuntando a raw_dlt.ingestion_log
        con = get_duckdb_connection(warehouse_path)
        try:
            con.execute("""
                CREATE OR REPLACE VIEW main.ingestion_log AS
                SELECT
                    file_name,
                    url,
                    row_count,
                    bytes,
                    sha256,
                    ingested_at,
                    status,
                    error_message,
                    _dlt_load_id
                FROM raw_dlt.ingestion_log;
            """)
            total_rows = con.execute(
                "SELECT COALESCE(SUM(row_count), 0) FROM main.ingestion_log WHERE file_name LIKE 'yellow_tripdata_%';"
            ).fetchone()[0]
        finally:
            con.close()

        print(f"[DLT OK] Paquete(s) cargados: {load_info.loads_ids}")
        return {
            "loads_ids": list(load_info.loads_ids),
            "total_rows_logged": int(total_rows),
        }

    except Exception as exc:
        print(f"[ERROR CRÍTICO INGESTA DLT] {exc}", file=sys.stderr)
        sys.exit(1)
