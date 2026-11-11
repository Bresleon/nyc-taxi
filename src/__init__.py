"""
nyc-taxi-pipeline: Pipeline Batch Reproducible NYC Yellow Taxi (TLC).

Ingesta idempotente, validación de contrato de esquema, compuerta de calidad
con cuarentena, almacenamiento columnar particionado (Hive) y modelado
dimensional estrella sobre DuckDB y dbt Core, orquestado con Dagster.
"""

from src.config import (
    CURATED_DIR,
    DATA_DIR,
    PROJECT_ROOT,
    QUARANTINE_DIR,
    RAW_DIR,
    RESULTADOS_PATH,
    SQL_DIR,
    WAREHOUSE_PATH,
    MonthPeriod,
    get_duckdb_connection,
    get_memory_usage_mb,
    parse_target_and_comparison_months,
)

__version__ = "1.0.0"
__author__ = "León Breslauer"
__all__ = [
    # Metadatos
    "__version__",
    "__author__",
    # Configuración y Rutas
    "MonthPeriod",
    "parse_target_and_comparison_months",
    "get_duckdb_connection",
    "get_memory_usage_mb",
    "PROJECT_ROOT",
    "DATA_DIR",
    "RAW_DIR",
    "QUARANTINE_DIR",
    "CURATED_DIR",
    "WAREHOUSE_PATH",
    "SQL_DIR",
    "RESULTADOS_PATH",
    # Etapa 1: Ingesta
    "compute_sha256",
    "init_ingestion_log",
    "ingest_month_parquet",
    "ingest_zone_lookup",
    # Etapa 2: Esquema
    "to_snake_case",
    "load_schema_contract",
    "SchemaValidationReport",
    "validate_and_standardize_month_schema",
    # Etapa 3: Calidad y Cuarentena
    "QualityRuleSpec",
    "QUALITY_RULES",
    "run_quality_gate_for_month",
    "reconcile_records",
    # Etapa 4: Transformación y Curada
    "write_curated_partition",
    # Etapa 5: Modelado y Consultas
    "build_star_schema_and_answer_queries",
    # Orquestación
    "execute_dagster_cli",
    "nyc_taxi_pipeline_job",
    "run_pipeline",
]


def run_pipeline(mes: str = "2025-03") -> None:
    """
    Punto de entrada programático para ejecutar el pipeline de punta a punta.
    Delega la ejecución al motor in-process de Dagster.

    Args:
        mes (str): Período objetivo en formato 'AAAA-MM' (ej. '2025-03').
    """
    # execute_dagster_cli(mes=mes)
