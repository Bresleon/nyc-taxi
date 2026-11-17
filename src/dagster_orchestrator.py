import json
import sys
import time
from pathlib import Path
from typing import Any, Dict, List

from dagster import (
    AssetExecutionContext,
    Config,
    Definitions,
    MetadataValue,
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
    get_duckdb_connection,
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
from src.telemetry import RAM_BUDGET_MB, track_stage_resources

OBSERVABILIDAD_PATH = PROJECT_ROOT / "OBSERVABILIDAD.md"


class PipelineRunConfig(Config):
    mes: str = "2025-03"


def _parse_dbt_run_results() -> List[Dict[str, Any]]:
    """
    Lee el artefacto target/run_results.json generado por dbt Core para extraer
    los tiempos de ejecución exactos por modelo y test dentro del subproceso.
    """
    run_results_file = PROJECT_ROOT / "dbt_project" / "target" / "run_results.json"
    if not run_results_file.exists():
        return []

    try:
        payload = json.loads(run_results_file.read_text(encoding="utf-8"))
        summary = []
        for res in payload.get("results", []):
            unique_id = res.get("unique_id", "")
            parts = unique_id.split(".")
            resource_type = parts[0] if len(parts) > 0 else "unknown"
            node_name = parts[-1] if len(parts) > 0 else unique_id
            summary.append(
                {
                    "tipo": resource_type,
                    "nodo": node_name,
                    "estado": res.get("status", "unknown"),
                    "tiempo_seg": round(float(res.get("execution_time", 0.0)), 3),
                }
            )
        return summary
    except Exception:
        return []


# ==============================================================================
# ASSET 1: Ingesta Idempotente (Archivos Crudos Parquet + Lookup CSV)
# ==============================================================================
@asset(
    group_name="1_ingestion",
    description="Descarga idempotente verificada por hash SHA-256 de los datos de TLC.",
    retry_policy=RetryPolicy(max_retries=3, delay=2),
)
def raw_taxi_data(
    context: AssetExecutionContext, config: PipelineRunConfig
) -> Dict[str, Any]:
    periods = parse_target_and_comparison_months(config.mes)
    run_short_id = context.run_id[:8]
    rows_counter = {"count": 0}
    results: Dict[str, Any] = {}

    with track_stage_resources(
        run_id=run_short_id,
        stage_name="1_ingestion_raw",
        period=config.mes,
        rows_processed_ref=rows_counter,
        warehouse_path=WAREHOUSE_PATH,
    ) as metric:
        con = get_duckdb_connection(WAREHOUSE_PATH)
        try:
            zones_path = ingest_zone_lookup(con, raw_base_dir=RAW_DIR)
            results["zones_csv"] = str(zones_path)

            downloaded_files = []
            for p in periods:
                p_path = ingest_month_parquet(con, p, raw_base_dir=RAW_DIR)
                downloaded_files.append(str(p_path))
            results["trip_files"] = downloaded_files

            # Obtener conteo total de filas registradas en ingestion_log
            total_ingested = con.execute(
                "SELECT COALESCE(SUM(row_count), 0) FROM ingestion_log WHERE file_name LIKE 'yellow_tripdata_%';"
            ).fetchone()[0]
            rows_counter["count"] = int(total_ingested)
        finally:
            con.close()

    context.add_output_metadata(
        {
            "mes_asignado": config.mes,
            "periodos_procesados": MetadataValue.text(
                ", ".join(p.label for p in periods)
            ),
            "filas_verificadas": MetadataValue.int(metric.rows_processed),
            "duracion_seg": MetadataValue.float(metric.duration_sec),
            "ram_pico_mb": MetadataValue.float(metric.ram_peak_mb),
            "ram_uso_sobre_8gb_pct": MetadataValue.float(metric.ram_pct_of_8gb),
            "cpu_user_seg": MetadataValue.float(metric.cpu_user_sec),
        }
    )
    return results


# ==============================================================================
# ASSET 2: Validación de Esquema, Calidad, Cuarentena y Capa Curada Hive
# ==============================================================================
@asset(
    deps=[raw_taxi_data],
    group_name="2_quality_and_curated",
    description="Contrato de esquema, 9 reglas de calidad, cuarentena y escritura Parquet Hive.",
)
def quality_validated_data(
    context: AssetExecutionContext, config: PipelineRunConfig
) -> Dict[str, Any]:
    periods = parse_target_and_comparison_months(config.mes)
    run_short_id = context.run_id[:8]
    rows_counter = {"count": 0}
    metrics_by_period: Dict[str, Any] = {}

    with track_stage_resources(
        run_id=run_short_id,
        stage_name="2_schema_quality_and_curated",
        period=config.mes,
        rows_processed_ref=rows_counter,
        warehouse_path=WAREHOUSE_PATH,
    ) as metric:
        con = get_duckdb_connection(WAREHOUSE_PATH)
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
                _, std_view = validate_and_standardize_month_schema(
                    con, p, parquet_path
                )

                accepted_view, q_metrics = run_quality_gate_for_month(
                    con,
                    p,
                    std_view,
                    zones_csv_path=zones_csv,
                    quarantine_base_dir=QUARANTINE_DIR,
                )
                metrics_by_period[p.label] = q_metrics

                write_curated_partition(
                    con, p, accepted_view, curated_base_dir=CURATED_DIR
                )

            rows_counter["count"] = sum(
                m["rows_read"] for m in metrics_by_period.values()
            )
        finally:
            con.close()

    context.add_output_metadata(
        {
            "resumen_calidad": MetadataValue.json(metrics_by_period),
            "filas_evaluadas": MetadataValue.int(metric.rows_processed),
            "throughput_filas_seg": MetadataValue.float(metric.throughput_rows_per_sec),
            "duracion_seg": MetadataValue.float(metric.duration_sec),
            "ram_pico_mb": MetadataValue.float(metric.ram_peak_mb),
            "ram_promedio_mb": MetadataValue.float(metric.ram_avg_mb),
            "ram_uso_sobre_8gb_pct": MetadataValue.float(metric.ram_pct_of_8gb),
            "spilling_disco_mb": MetadataValue.float(metric.temp_spill_mb),
            "cpu_user_seg": MetadataValue.float(metric.cpu_user_sec),
        }
    )
    return metrics_by_period


# ==============================================================================
# ASSET 3: Modelo Dimensional y Marts (dbt Core en 3 Capas)
# ==============================================================================
@asset(
    deps=[quality_validated_data],
    group_name="3_dbt_dimensional_modeling",
    description="Ejecuta dbt run y dbt test materializando staging, intermediate y marts.",
)
def dimensional_star_schema(
    context: AssetExecutionContext, config: PipelineRunConfig
) -> Dict[str, int]:
    periods = parse_target_and_comparison_months(config.mes)
    run_short_id = context.run_id[:8]
    rows_counter = {"count": 0}

    with track_stage_resources(
        run_id=run_short_id,
        stage_name="3_dbt_staging_and_marts",
        period=config.mes,
        rows_processed_ref=rows_counter,
        warehouse_path=WAREHOUSE_PATH,
    ) as metric:
        # 1. dbt abre nyc_taxi.duckdb de forma exclusiva para crear staging/intermediate/marts
        run_dbt_transformations(periods)

        # 2. Una vez cerrado dbt, Python consulta marts y exporta RESULTADOS.md
        counts = run_business_queries_and_export_md(
            periods,
            warehouse_path=WAREHOUSE_PATH,
            sql_dir=SQL_DIR,
            resultados_path=RESULTADOS_PATH,
        )
        rows_counter["count"] = int(counts["fact_trips"])

    dbt_nodes_timing = _parse_dbt_run_results()
    dbt_md_lines = [
        "| Tipo | Nodo dbt | Estado | Tiempo (s) |",
        "| :--- | :--- | :--- | ---: |",
    ]
    for item in dbt_nodes_timing:
        dbt_md_lines.append(
            f"| `{item['tipo']}` | `{item['nodo']}` | {item['estado']} | {item['tiempo_seg']} |"
        )

    context.add_output_metadata(
        {
            "fact_trips_rows": MetadataValue.int(counts["fact_trips"]),
            "agg_zone_hour_rows": MetadataValue.int(counts["agg_zone_hour"]),
            "dim_zone_count": MetadataValue.int(counts["dim_zone"]),
            "dim_date_count": MetadataValue.int(counts["dim_date"]),
            "throughput_filas_seg": MetadataValue.float(metric.throughput_rows_per_sec),
            "duracion_total_seg": MetadataValue.float(metric.duration_sec),
            "ram_pico_proceso_padre_mb": MetadataValue.float(metric.ram_peak_mb),
            "ram_uso_sobre_8gb_pct": MetadataValue.float(metric.ram_pct_of_8gb),
            "perfil_nodos_dbt": MetadataValue.md("\n".join(dbt_md_lines)),
        }
    )
    return counts


# ==============================================================================
# ASSET 4: Auditoría Final de Observabilidad y Exportación de Reportes
# ==============================================================================
@asset(
    deps=[dimensional_star_schema],
    group_name="4_observability_and_reporting",
    description="Consolida métricas de recursos en OBSERVABILIDAD.md y valida RESULTADOS.md.",
)
def business_results_and_observability(
    context: AssetExecutionContext, config: PipelineRunConfig
) -> str:
    if not RESULTADOS_PATH.exists():
        raise FileNotFoundError(f"No se encontró el archivo {RESULTADOS_PATH}")

    run_short_id = context.run_id[:8]
    con = get_duckdb_connection(WAREHOUSE_PATH)
    try:
        rows = con.execute(
            """
            SELECT
                stage_name,
                duration_sec,
                ram_start_mb,
                ram_peak_mb,
                ram_avg_mb,
                ram_pct_of_8gb,
                cpu_user_sec,
                cpu_sys_sec,
                temp_spill_mb,
                rows_processed,
                throughput_rows_per_sec
            FROM main.pipeline_metrics_log
            WHERE run_id = ?
            ORDER BY started_at ASC;
            """,
            [run_short_id],
        ).fetchall()
    finally:
        con.close()

    max_ram_mb = max((r[3] for r in rows), default=0.0)
    max_ram_pct = round((max_ram_mb / RAM_BUDGET_MB) * 100.0, 2)
    total_dur_sec = round(sum(r[1] for r in rows), 2)

    # Construir tabla Markdown de observabilidad
    md_lines = [
        f"# Reporte de Observabilidad y Uso de Recursos (`MES={config.mes}`)\n",
        f"- **Run ID (Dagster):** `{run_short_id}`\n"
        f"- **Presupuesto de Hardware Evaluado:** `8,192.00 MB` (8.00 GB RAM)\n"
        f"- **Pico Máximo Global de RAM (RSS):** `{max_ram_mb:.2f} MB` (`{max_ram_pct}%` del límite de 8 GB)\n"
        f"- **Tiempo Total Medido por Etapas:** `{total_dur_sec:.2f} s`\n",
        "## Desglose por Etapa del Pipeline\n",
        "| Etapa | Duración (s) | RAM Inicio (MB) | RAM Pico (MB) | % de 8 GB | CPU User (s) | Spill Disco (MB) | Filas | Throughput (filas/s) |",
        "| :--- | ---: | ---: | ---: | ---: | ---: | ---: | ---: | ---: |",
    ]

    print("\n" + "=" * 96)
    print(
        f"PERFIL DE RECURSOS DEL PIPELINE (RUN: {run_short_id} | TECHO: 8,192 MB RAM)"
    )
    print("=" * 96)
    print(
        f"{'Etapa':<30} | {'Duración':>9} | {'RAM Pico':>10} | {'% 8GB':>7} | {'Throughput':>18}"
    )
    print("-" * 96)

    for r in rows:
        stage, dur, r_ini, r_peak, r_avg, r_pct, cpu_u, cpu_s, spill, n_rows, tput = r
        tput_str = f"{tput:,.0f} filas/s" if n_rows > 0 else "-"
        print(
            f"{stage:<30} | {dur:>7.2f} s | {r_peak:>7.2f} MB | {r_pct:>6.2f}% | {tput_str:>18}"
        )
        md_lines.append(
            f"| `{stage}` | {dur:.2f} | {r_ini:.2f} | **{r_peak:.2f}** | {r_pct:.2f}% | "
            f"{cpu_u:.2f} | {spill:.2f} | {n_rows:,} | {tput_str} |"
        )

    print("=" * 96 + "\n")

    OBSERVABILIDAD_PATH.write_text("\n".join(md_lines) + "\n", encoding="utf-8")

    context.add_output_metadata(
        {
            "resultados_path": MetadataValue.text(str(RESULTADOS_PATH)),
            "observabilidad_path": MetadataValue.text(str(OBSERVABILIDAD_PATH)),
            "pico_global_ram_mb": MetadataValue.float(max_ram_mb),
            "pico_global_ram_pct_8gb": MetadataValue.float(max_ram_pct),
            "tabla_observabilidad": MetadataValue.md("\n".join(md_lines)),
        }
    )
    return str(OBSERVABILIDAD_PATH)


# ==============================================================================
# DEFINICIONES Y JOB PRINCIPAL DE DAGSTER
# ==============================================================================
all_pipeline_assets = [
    raw_taxi_data,
    quality_validated_data,
    dimensional_star_schema,
    business_results_and_observability,
]

nyc_taxi_pipeline_job = define_asset_job(
    name="nyc_taxi_pipeline_job",
    selection=all_pipeline_assets,
    description="Pipeline batch E2E NYC Taxi gobernado por Dagster con observabilidad nativa.",
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
            "business_results_and_observability": {"config": {"mes": mes}},
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
