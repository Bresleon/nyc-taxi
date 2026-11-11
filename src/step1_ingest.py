import hashlib
import os
import sys
import time
from datetime import datetime, timezone
from pathlib import Path
from typing import Optional

import duckdb
import pyarrow.parquet as pq
import requests

from src.config import (
    RAW_DIR,
    TLC_TRIP_URL_TEMPLATE,
    TLC_ZONE_LOOKUP_URL,
    MonthPeriod,
)


def compute_sha256(file_path: Path) -> str:
    """Calcula el hash SHA-256 de un archivo en bloques de 64KB (O(1) RAM)."""
    sha256 = hashlib.sha256()
    with open(file_path, "rb") as f:
        for chunk in iter(lambda: f.read(65536), b""):
            sha256.update(chunk)
    return sha256.hexdigest()


def init_ingestion_log(con: duckdb.DuckDBPyConnection) -> None:
    """Crea la tabla de control ingestion_log si no existe."""
    con.execute("""
        CREATE TABLE IF NOT EXISTS ingestion_log (
            file_name VARCHAR PRIMARY KEY,
            url VARCHAR,
            row_count BIGINT,
            bytes BIGINT,
            sha256 VARCHAR,
            ingested_at TIMESTAMP,
            status VARCHAR,
            error_message VARCHAR
        );
    """)


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


def _record_log(
    con: duckdb.DuckDBPyConnection,
    file_name: str,
    url: str,
    row_count: int,
    file_bytes: int,
    sha256: Optional[str],
    status: str,
    error_message: Optional[str] = None,
) -> None:
    """Registra de forma idempotente (INSERT OR REPLACE) el resultado en ingestion_log."""
    now_utc = datetime.now(timezone.utc).replace(tzinfo=None)
    con.execute(
        """
        INSERT OR REPLACE INTO ingestion_log
        (file_name, url, row_count, bytes, sha256, ingested_at, status, error_message)
        VALUES (?, ?, ?, ?, ?, ?, ?, ?);
        """,
        [file_name, url, row_count, file_bytes, sha256, now_utc, status, error_message],
    )


def ingest_month_parquet(
    con: duckdb.DuckDBPyConnection,
    period: MonthPeriod,
    raw_base_dir: Path = RAW_DIR,
    max_retries: int = 3,
    base_wait: float = 2.0,
) -> Path:
    """
    Descarga el archivo Parquet de un mes a data/raw/yellow/year=AAAA/month=MM/
    garantizando idempotencia por verificación de existencia y hash SHA-256.
    """
    init_ingestion_log(con)
    url = TLC_TRIP_URL_TEMPLATE.format(year=period.year, month=period.month)
    dest_dir = (
        raw_base_dir / "yellow" / f"year={period.year}" / f"month={period.month:02d}"
    )
    dest_path = dest_dir / period.file_name

    # 1. Verificación de Idempotencia
    if dest_path.exists():
        disk_hash = compute_sha256(dest_path)
        logged = con.execute(
            """
            SELECT sha256, row_count, bytes
            FROM ingestion_log
            WHERE file_name = ? AND status = 'ok'
            """,
            [period.file_name],
        ).fetchone()

        if logged and logged[0] == disk_hash:
            print(
                f"[INGESTA] Idempotente OK: '{period.file_name}' ya existe y su hash "
                f"SHA-256 ({disk_hash[:12]}...) coincide ({logged[1]:,} filas). No se re-descarga."
            )
            return dest_path
        else:
            # El archivo ya estaba en disco (ej. pre-descargado) pero aún no en ingestion_log
            try:
                row_count = pq.read_metadata(dest_path).num_rows
                file_bytes = dest_path.stat().st_size
                _record_log(
                    con, period.file_name, url, row_count, file_bytes, disk_hash, "ok"
                )
                print(
                    f"[INGESTA] Archivo local existente validado y registrado: "
                    f"'{period.file_name}' ({row_count:,} filas)."
                )
                return dest_path
            except Exception:
                print(
                    f"[INGESTA] Archivo local '{period.file_name}' corrupto o incompleto. Re-descargando..."
                )

    # 2. Descarga con reintentos y registro
    try:
        print(f"[INGESTA] Descargando {url} -> {dest_path} ...")
        download_file_with_backoff(
            url, dest_path, max_retries=max_retries, base_wait=base_wait
        )
        disk_hash = compute_sha256(dest_path)
        file_bytes = dest_path.stat().st_size
        # Leemos únicamente los metadatos del footer Parquet (no carga las filas en RAM)
        row_count = pq.read_metadata(dest_path).num_rows

        _record_log(con, period.file_name, url, row_count, file_bytes, disk_hash, "ok")
        print(
            f"[INGESTA] Completada '{period.file_name}': {row_count:,} filas | "
            f"{file_bytes / (1024 * 1024):.2f} MB | SHA-256: {disk_hash[:16]}..."
        )
        return dest_path

    except Exception as exc:
        _record_log(con, period.file_name, url, 0, 0, None, "error", str(exc))
        print(f"[ERROR CRÍTICO INGESTA] {exc}", file=sys.stderr)
        sys.exit(1)


def ingest_zone_lookup(
    con: duckdb.DuckDBPyConnection,
    raw_base_dir: Path = RAW_DIR,
    max_retries: int = 3,
    base_wait: float = 2.0,
) -> Path:
    """
    Descarga la tabla maestra taxi_zone_lookup.csv una única vez de manera idempotente.
    """
    init_ingestion_log(con)
    file_name = "taxi_zone_lookup.csv"
    dest_path = raw_base_dir / "zones" / file_name

    if dest_path.exists():
        disk_hash = compute_sha256(dest_path)
        logged = con.execute(
            "SELECT sha256, row_count FROM ingestion_log WHERE file_name = ? AND status = 'ok'",
            [file_name],
        ).fetchone()
        if logged and logged[0] == disk_hash:
            print(
                f"[INGESTA] Idempotente OK: '{file_name}' ya existe ({logged[1]} zonas)."
            )
            return dest_path

    try:
        print(f"[INGESTA] Descargando tabla de zonas {TLC_ZONE_LOOKUP_URL} ...")
        download_file_with_backoff(
            TLC_ZONE_LOOKUP_URL, dest_path, max_retries=max_retries, base_wait=base_wait
        )
        disk_hash = compute_sha256(dest_path)
        file_bytes = dest_path.stat().st_size
        with open(dest_path, "rb") as f:
            row_count = max(0, sum(1 for _ in f) - 1)

        _record_log(
            con, file_name, TLC_ZONE_LOOKUP_URL, row_count, file_bytes, disk_hash, "ok"
        )
        print(f"[INGESTA] Completada '{file_name}': {row_count} zonas.")
        return dest_path
    except Exception as exc:
        _record_log(con, file_name, TLC_ZONE_LOOKUP_URL, 0, 0, None, "error", str(exc))
        print(f"[ERROR CRÍTICO INGESTA ZONAS] {exc}", file=sys.stderr)
        sys.exit(1)
