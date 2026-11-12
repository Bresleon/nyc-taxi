"""
Paso 2 — Contrato de esquema y evolución.

- Lee contracts/yellow_schema.yaml.
- Inspecciona el esquema físico real del Parquet con DESCRIBE.
- Reporta columnas faltantes, nuevas y diferencias de tipo (schema_audit_log).
- Crea una vista temporal estandarizada (snake_case + tipos del contrato).
  Las columnas opcionales faltantes (cbd_congestion_fee en 2024) se inyectan
  con su default_value y las columnas nuevas no contratadas se ignoran.
"""

import re
from dataclasses import dataclass, field
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Dict, List, Tuple

import duckdb
import yaml

from src.config import CONTRACT_PATH, MonthPeriod


@dataclass
class SchemaValidationReport:
    file_name: str
    missing_required_columns: List[str] = field(default_factory=list)
    missing_optional_columns: List[str] = field(default_factory=list)
    new_unexpected_columns: List[str] = field(default_factory=list)
    type_differences: List[str] = field(default_factory=list)
    is_valid: bool = True


def to_snake_case(name: str) -> str:
    """
    Normaliza nombres de columnas de TLC a snake_case.
      'PULocationID' -> 'pu_location_id'
      'VendorID'     -> 'vendor_id'
      'Airport_fee'  -> 'airport_fee'
    """
    special_mappings = {
        "PULocationID": "pu_location_id",
        "DOLocationID": "do_location_id",
        "VendorID": "vendor_id",
        "RatecodeID": "ratecode_id",
        "Airport_fee": "airport_fee",
    }
    if name in special_mappings:
        return special_mappings[name]

    s1 = re.sub(r"([A-Z]+)([A-Z][a-z])", r"\1_\2", name)
    s2 = re.sub(r"([a-z0-9])([A-Z])", r"\1_\2", s1)
    return re.sub(r"_+", "_", s2).lower()


def load_schema_contract(contract_path: Path = CONTRACT_PATH) -> Dict[str, Any]:
    with open(contract_path, "r", encoding="utf-8") as f:
        return yaml.safe_load(f)


def _classify_duckdb_type_family(dtype_str: str) -> str:
    """Agrupa tipos físicos en familias compatibles (INT32 e INT64 -> INTEGER, etc.)."""
    dtype_upper = dtype_str.upper().strip('"')
    if dtype_upper == "NULL":  # columna 100% vacía en el Parquet: se castea a cualquier tipo
        return "NULL"
    if "TIMESTAMP" in dtype_upper or "DATE" in dtype_upper:
        return "TIMESTAMP"
    if "INT" in dtype_upper:  # TINYINT, SMALLINT, INTEGER, BIGINT, HUGEINT
        return "INTEGER"
    if any(t in dtype_upper for t in ["DOUBLE", "FLOAT", "DECIMAL", "NUMERIC", "REAL"]):
        return "NUMERIC"
    if any(t in dtype_upper for t in ["VARCHAR", "CHAR", "STRING", "TEXT"]):
        return "STRING"
    return "OTHER"


def _is_compatible(actual_family: str, expected_family: str) -> bool:
    """Un entero siempre se puede castear a DOUBLE sin pérdida (upcasting seguro)."""
    if actual_family == expected_family or actual_family == "NULL":
        return True
    return actual_family == "INTEGER" and expected_family == "NUMERIC"


def init_schema_audit_table(con: duckdb.DuckDBPyConnection) -> None:
    con.execute("""
        CREATE TABLE IF NOT EXISTS schema_audit_log (
            file_name VARCHAR PRIMARY KEY,
            checked_at TIMESTAMP,
            missing_required VARCHAR,
            missing_optional VARCHAR,
            new_columns VARCHAR,
            type_differences VARCHAR,
            is_valid BOOLEAN
        );
    """)


def validate_and_standardize_month_schema(
    con: duckdb.DuckDBPyConnection,
    period: MonthPeriod,
    parquet_path: Path,
    contract_path: Path = CONTRACT_PATH,
) -> Tuple[SchemaValidationReport, str]:
    """
    1. Compara el esquema físico del Parquet contra el contrato.
    2. Registra columnas faltantes, nuevas y cambios de tipo en schema_audit_log.
    3. Crea la vista temporal v_standardized_AAAA_MM con nombres snake_case y
       tipos casteados al contrato. Devuelve (reporte, nombre_de_la_vista).
    """
    init_schema_audit_table(con)
    contract = load_schema_contract(contract_path)
    report = SchemaValidationReport(file_name=period.file_name)

    # Esquema físico real del Parquet (solo lee metadatos, no las filas)
    raw_cols_info = con.execute(
        f"DESCRIBE SELECT * FROM read_parquet('{parquet_path.as_posix()}');"
    ).fetchall()
    actual_columns: Dict[str, str] = {row[0]: row[1] for row in raw_cols_info}
    actual_lower_map: Dict[str, str] = {row[0].lower(): row[0] for row in raw_cols_info}

    matched_actual_cols = set()
    select_expressions: List[str] = []

    for col_spec in contract["columns"]:
        target_name = col_spec["target_name"]
        expected_type = col_spec["expected_type"]
        # Si el contrato no trae type_family, se deduce del expected_type
        expected_family = col_spec.get(
            "type_family", _classify_duckdb_type_family(expected_type)
        )
        is_required = col_spec.get("required", True)
        default_val = col_spec.get("default_value", None)

        # Buscar la columna por nombre original, alias o nombre destino (sin importar mayúsculas)
        candidates = [col_spec["source_name"]] + (col_spec.get("aliases") or []) + [target_name]
        found_orig_name = None
        for cand in candidates:
            if cand in actual_columns:
                found_orig_name = cand
                break
            if cand.lower() in actual_lower_map:
                found_orig_name = actual_lower_map[cand.lower()]
                break

        if found_orig_name is not None:
            matched_actual_cols.add(found_orig_name)
            actual_type = actual_columns[found_orig_name]
            actual_family = _classify_duckdb_type_family(actual_type)

            if actual_type.upper() != expected_type.upper():
                if not _is_compatible(actual_family, expected_family):
                    report.type_differences.append(
                        f"{found_orig_name}: incompatible ({actual_type} vs esperado {expected_type})"
                    )
                    report.is_valid = False
                else:
                    report.type_differences.append(
                        f"{found_orig_name}: {actual_type} -> casteado a {expected_type}"
                    )

            if not is_required and default_val is not None:
                # Columna opcional presente: los NULL se reemplazan por el default del contrato
                select_expressions.append(
                    f'COALESCE(CAST("{found_orig_name}" AS {expected_type}), '
                    f"CAST({default_val} AS {expected_type})) AS {target_name}"
                )
            else:
                select_expressions.append(
                    f'CAST("{found_orig_name}" AS {expected_type}) AS {target_name}'
                )
        else:
            if is_required:
                report.missing_required_columns.append(target_name)
                report.is_valid = False
            else:
                # Ej.: cbd_congestion_fee no existe en 2024 -> se inyecta 0.0
                report.missing_optional_columns.append(target_name)
                val_sql = "NULL" if default_val is None else str(default_val)
                select_expressions.append(
                    f"CAST({val_sql} AS {expected_type}) AS {target_name}"
                )

    # Columnas nuevas que no están en el contrato (schema drift)
    for actual_col, actual_type in actual_columns.items():
        if actual_col not in matched_actual_cols:
            report.new_unexpected_columns.append(
                f"{actual_col} (snake_case: {to_snake_case(actual_col)}, tipo: {actual_type})"
            )
            if contract.get("extra_columns_policy") == "fail":
                report.is_valid = False

    # Auditoría persistida en DuckDB (1 fila por archivo, idempotente)
    now_utc = datetime.now(timezone.utc).replace(tzinfo=None)
    con.execute(
        "INSERT OR REPLACE INTO schema_audit_log VALUES (?, ?, ?, ?, ?, ?, ?);",
        [
            period.file_name,
            now_utc,
            ", ".join(report.missing_required_columns) or "Ninguna",
            ", ".join(report.missing_optional_columns) or "Ninguna",
            ", ".join(report.new_unexpected_columns) or "Ninguna",
            "; ".join(report.type_differences) or "Ninguna",
            report.is_valid,
        ],
    )

    print(f"[ESQUEMA] Reporte para '{period.file_name}':")
    print(f"  - Columnas obligatorias faltantes : {report.missing_required_columns or 'Ninguna'}")
    print(f"  - Columnas evolutivas faltantes   : {report.missing_optional_columns or 'Ninguna'}")
    print(f"  - Columnas nuevas no contratadas  : {report.new_unexpected_columns or 'Ninguna'}")
    print(f"  - Adaptaciones de tipo físico     : {report.type_differences or 'Ninguna'}")

    if not report.is_valid:
        raise ValueError(
            f"El archivo '{period.file_name}' violó el contrato de esquema obligatorio: {report}"
        )

    # Vista estandarizada del mes (solo columnas del contrato, en orden del contrato)
    view_name = f"v_standardized_{period.year}_{period.month:02d}"
    projection_sql = ",\n            ".join(select_expressions)
    con.execute(f"""
        CREATE OR REPLACE TEMP VIEW {view_name} AS
        SELECT
            {projection_sql}
        FROM read_parquet('{parquet_path.as_posix()}');
    """)

    return report, view_name
