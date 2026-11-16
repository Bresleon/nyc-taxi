import os
import sys
import time
from pathlib import Path
from typing import Any, Dict, List

import duckdb
from dagster import (
    AssetExecutionContext,
    Config,
    Definitions,
    MetadataValue,
    OpExecutionContext,
    RetryPolicy,
    asset,
    define_asset_job,
)

from src.config import (
    CURATED_DIR,
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
from src.step1_ingest import ingest_month_parquet, ingest_zone_lookup
from src.step2_schema import validate_and_standardize_month_schema
from src.step3_quality import run_quality_gate_for_month
from src.step4_transform import write_curated_partition
from src.step5_model_and_queries import (
    run_business_queries_and_export_md,
    run_dbt_transformations,
)


class PipelineRunConfig(Config):
    mes: str = "2025-03"


# ==============================================================================
# ASSET 1: Ingesta Idempotente (Archivos Crudos Parquet + Lookup CSV)
# ==============================================================================
@asset(
    group_name="ingestion",
    description="Descarga idempotente verificada por hash SHA-256 de los datos de TLC.",
    retry_policy=RetryPolicy(max_retries=3, delay=2),
)
def raw_taxi_data(
    context: AssetExecutionContext, config: PipelineRunConfig
) -> Dict[str, Any]:
    periods = parse_target_and_comparison_months(config.mes)
    con = get_duckdb_connection(WAREHOUSE_PATH)
    results = {}
    try:
        zones_path = ingest_zone_lookup(con, raw_base_dir=RAW_DIR)
        results["zones_csv"] = str(zones_path)

        downloaded_files = []
        for p in periods:
            p_path = ingest_month_parquet(con, p, raw_base_dir=RAW_DIR)
            downloaded_files.append(str(p_path))
        results["trip_files"] = downloaded_files

        context.add_output_metadata(
            {
                "mes_asignado": config.mes,
                "periodos_procesados": MetadataValue.text(
                    ", ".join(p.label for p in periods)
                ),
                "archivos_descargados": MetadataValue.json(downloaded_files),
                "ram_rss_mb": MetadataValue.float(get_memory_usage_mb()),
            }
        )
    finally:
        con.close()
    return results


# ==============================================================================
# ASSET 2: Validación de Contrato de Esquema y Cuarentena de Calidad
# ==============================================================================
@asset(
    deps=[raw_taxi_data],
    group_name="quality_gate",
    description="Aplica contrato de esquema y las 9 reglas de calidad separando aceptadas y rechazadas.",
)
def quality_validated_data(
    context: AssetExecutionContext, config: PipelineRunConfig
) -> Dict[str, Any]:
    periods = parse_target_and_comparison_months(config.mes)
    con = get_duckdb_connection(WAREHOUSE_PATH)
    metrics_by_period = {}
    try:
        zones_csv = RAW_DIR / "zones" / "taxi_zone_lookup.csv"
        for p in periods:
            parquet_path = (
                RAW_DIR
                / "yellow"
                / f"year={p.year}"
                / f"month={p.month:02d}"
                / p.file_name
            )
            # Validación de Esquema
            _, std_view = validate_and_standardize_month_schema(con, p, parquet_path)

            # Compuerta de Calidad, Cuarentena y Reconciliación
            accepted_view, metrics = run_quality_gate_for_month(
                con,
                p,
                std_view,
                zones_csv_path=zones_csv,
                quarantine_base_dir=QUARANTINE_DIR,
            )
            metrics_by_period[p.label] = metrics

            # Escritura en la Capa Curada Particionada (Hive)
            write_curated_partition(con, p, accepted_view, curated_base_dir=CURATED_DIR)

        context.add_output_metadata(
            {
                "resumen_calidad": MetadataValue.json(metrics_by_period),
                "ram_rss_mb": MetadataValue.float(get_memory_usage_mb()),
            }
        )
    finally:
        con.close()
    return metrics_by_period


# ==============================================================================
# ASSET 3: Modelo Dimensional y Marts (dbt Core)
# ==============================================================================
@asset(
    deps=[quality_validated_data],
    group_name="transformation_and_modeling",
    description="Construcción y testeo del modelo dimensional estrella usando dbt Core.",
)
def dimensional_star_schema(
    context: AssetExecutionContext, config: PipelineRunConfig
) -> Dict[str, int]:
    periods = parse_target_and_comparison_months(config.mes)

    # 1. dbt abre nyc_taxi.duckdb en modo exclusivo, crea las tablas y corre dbt test
    run_dbt_transformations(periods)

    # 2. Una vez cerrado dbt, Python consulta el modelo y exporta RESULTADOS.md
    counts = run_business_queries_and_export_md(
        periods,
        warehouse_path=WAREHOUSE_PATH,
        sql_dir=SQL_DIR,
        resultados_path=RESULTADOS_PATH,
    )

    context.add_output_metadata(
        {
            "fact_trips_rows": MetadataValue.int(counts["fact_trips"]),
            "agg_zone_hour_rows": MetadataValue.int(counts["agg_zone_hour"]),
            "dim_zone_count": MetadataValue.int(counts["dim_zone"]),
            "dim_date_count": MetadataValue.int(counts["dim_date"]),
            "ram_rss_mb": MetadataValue.float(get_memory_usage_mb()),
        }
    )
    return counts


# ==============================================================================
# ASSET 4: Reporte Final de Negocio (RESULTADOS.md)
# ==============================================================================
@asset(
    deps=[dimensional_star_schema],
    group_name="reporting",
    description="Verificación de la generación de RESULTADOS.md con las 6 preguntas analíticas.",
)
def business_results_markdown(context: AssetExecutionContext) -> str:
    if not RESULTADOS_PATH.exists():
        raise FileNotFoundError(f"No se encontró el archivo {RESULTADOS_PATH}")

    file_size_kb = RESULTADOS_PATH.stat().st_size / 1024.0
    context.add_output_metadata(
        {
            "path": MetadataValue.text(str(RESULTADOS_PATH)),
            "tamano_kb": MetadataValue.float(file_size_kb),
        }
    )
    return str(RESULTADOS_PATH)


# ==============================================================================
# DEFINICIONES Y JOB PRINCIPAL DE DAGSTER
# ==============================================================================
all_pipeline_assets = [
    raw_taxi_data,
    quality_validated_data,
    dimensional_star_schema,
    business_results_markdown,
]

nyc_taxi_pipeline_job = define_asset_job(
    name="nyc_taxi_pipeline_job",
    selection=all_pipeline_assets,
    description="Pipeline batch E2E NYC Taxi gobernado por Dagster.",
)

defs = Definitions(
    assets=all_pipeline_assets,
    jobs=[nyc_taxi_pipeline_job],
)


# ==============================================================================
# ENTRYPOINT CLI: Ejecución controlada por Dagster en proceso
# ==============================================================================
def execute_dagster_cli(mes: str) -> None:
    """
    Resuelve el job compilado dentro de Definitions y lo ejecuta usando
    'execute_in_process'. Cumple con correr con un solo comando sin demonios.
    """
    print("=" * 80)
    print(f"[DAGSTER] Iniciando ejecución del grafo de assets para el mes: {mes}")
    print("=" * 80)

    run_config = {
        "ops": {
            "raw_taxi_data": {"config": {"mes": mes}},
            "quality_validated_data": {"config": {"mes": mes}},
            "dimensional_star_schema": {"config": {"mes": mes}},
        }
    }

    t0 = time.perf_counter()
    resolved_job = defs.resolve_job_def("nyc_taxi_pipeline_job")
    result = resolved_job.execute_in_process(run_config=run_config)
    elapsed = time.perf_counter() - t0

    if not result.success:
        print(
            "[DAGSTER ERROR] La corrida falló en alguno de los assets.", file=sys.stderr
        )
        sys.exit(1)

    print("=" * 80)
    print(
        f"[DAGSTER OK] Todos los assets fueron materializados con éxito en {elapsed:.2f} s."
    )
    print("=" * 80)


if __name__ == "__main__":
    import argparse

    parser = argparse.ArgumentParser(
        description="Dagster Runner para NYC Taxi Pipeline"
    )
    parser.add_argument(
        "--mes", type=str, default="2025-03", help="Mes asignado (AAAA-MM)"
    )
    args = parser.parse_args()
    execute_dagster_cli(mes=args.mes)
