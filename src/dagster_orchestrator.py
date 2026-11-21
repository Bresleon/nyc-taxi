import json
import sys
import time
from datetime import datetime
from pathlib import Path
from typing import Any, Dict, List

from dagster import (
    AssetExecutionContext,
    Config,
    Definitions,
    MetadataValue,
    RetryPolicy,
    RunRequest,
    ScheduleEvaluationContext,
    SensorEvaluationContext,
    SkipReason,
    asset,
    define_asset_job,
    schedule,
    sensor,
)

from src.config import (
    CURATED_DIR,
    DATA_DIR,
    PROJECT_ROOT,
    QUARANTINE_DIR,
    RAW_DIR,
    RESULTADOS_PATH,
    SQL_DIR,
    WAREHOUSE_PATH,
    get_duckdb_connection,
    parse_target_and_comparison_months,
)
from src.step1_ingest import run_dlt_ingestion
from src.step2_schema import validate_and_standardize_month_schema
from src.step3_quality import run_quality_gate_for_month
from src.step4_transform import write_curated_partition
from src.step5_model_and_queries import (
    run_business_queries_and_export_md,
    run_dbt_transformations,
)

TRIGGERS_DIR = DATA_DIR / "triggers"


class PipelineRunConfig(Config):
    mes: str = "2025-03"


def _parse_dbt_run_results() -> List[Dict[str, Any]]:
    """Extrae tiempos de ejecución por modelo y test desde target/run_results.json."""
    run_results_file = PROJECT_ROOT / "dbt-project" / "target" / "run_results.json"
    if not run_results_file.exists():
        return []
    try:
        payload = json.loads(run_results_file.read_text(encoding="utf-8"))
        summary = []
        for res in payload.get("results", []):
            parts = res.get("unique_id", "").split(".")
            summary.append(
                {
                    "tipo": parts[0] if parts else "unknown",
                    "nodo": parts[-1] if parts else res.get("unique_id", ""),
                    "estado": res.get("status", "unknown"),
                    "tiempo_seg": round(float(res.get("execution_time", 0.0)), 3),
                }
            )
        return summary
    except Exception:
        return []


# ==============================================================================
# ASSET 1: Ingesta Idempotente con dlt (data load tool)
# ==============================================================================
@asset(
    group_name="1_ingestion",
    description="Ingesta idempotente gobernada por dlt con trazabilidad en _dlt_loads e ingestion_log.",
    retry_policy=RetryPolicy(max_retries=3, delay=2),
)
def raw_taxi_data(
    context: AssetExecutionContext, config: PipelineRunConfig
) -> Dict[str, Any]:
    periods = parse_target_and_comparison_months(config.mes)
    context.log.info(
        f"Iniciando ingesta dlt para períodos: {[p.label for p in periods]}"
    )

    dlt_result = run_dlt_ingestion(
        periods=periods,
        raw_base_dir=RAW_DIR,
        warehouse_path=WAREHOUSE_PATH,
    )

    context.add_output_metadata(
        {
            "mes_asignado": config.mes,
            "periodos_procesados": MetadataValue.text(
                ", ".join(p.label for p in periods)
            ),
            "dlt_loads_ids": MetadataValue.json(dlt_result["loads_ids"]),
            "filas_registradas": MetadataValue.int(dlt_result["total_rows_logged"]),
        }
    )
    return dlt_result


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
    metrics_by_period: Dict[str, Any] = {}

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
            _, std_view = validate_and_standardize_month_schema(con, p, parquet_path)

            accepted_view, q_metrics = run_quality_gate_for_month(
                con,
                p,
                std_view,
                zones_csv_path=zones_csv,
                quarantine_base_dir=QUARANTINE_DIR,
            )
            metrics_by_period[p.label] = q_metrics
            write_curated_partition(con, p, accepted_view, curated_base_dir=CURATED_DIR)
    finally:
        con.close()

    context.add_output_metadata(
        {
            "resumen_calidad": MetadataValue.json(metrics_by_period),
            "total_filas_leidas": MetadataValue.int(
                sum(m["rows_read"] for m in metrics_by_period.values())
            ),
            "total_filas_aceptadas": MetadataValue.int(
                sum(m["rows_accepted"] for m in metrics_by_period.values())
            ),
            "total_filas_cuarentena": MetadataValue.int(
                sum(m["rows_quarantined"] for m in metrics_by_period.values())
            ),
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

    run_dbt_transformations(periods)
    counts = run_business_queries_and_export_md(
        periods,
        warehouse_path=WAREHOUSE_PATH,
        sql_dir=SQL_DIR,
        resultados_path=RESULTADOS_PATH,
    )

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
            "perfil_nodos_dbt": MetadataValue.md("\n".join(dbt_md_lines)),
        }
    )
    return counts


# ==============================================================================
# ASSET 4: Verificación de Entregable de Negocio (RESULTADOS.md)
# ==============================================================================
@asset(
    deps=[dimensional_star_schema],
    group_name="4_reporting",
    description="Valida la actualización de RESULTADOS.md con las 6 preguntas analíticas.",
)
def business_results_markdown(
    context: AssetExecutionContext, config: PipelineRunConfig
) -> str:
    if not RESULTADOS_PATH.exists():
        raise FileNotFoundError(f"No se encontró el archivo {RESULTADOS_PATH}")

    size_kb = round(RESULTADOS_PATH.stat().st_size / 1024.0, 2)
    context.add_output_metadata(
        {
            "mes_procesado": config.mes,
            "resultados_path": MetadataValue.text(str(RESULTADOS_PATH)),
            "tamano_kb": MetadataValue.float(size_kb),
        }
    )
    return str(RESULTADOS_PATH)


# ==============================================================================
# JOB, SCHEDULE Y SENSOR (Orquestación Viva)
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
    description="Pipeline batch E2E NYC Taxi gobernado por Dagster + dlt + dbt.",
)


def _build_run_config(mes: str) -> Dict[str, Any]:
    return {
        "ops": {
            "raw_taxi_data": {"config": {"mes": mes}},
            "quality_validated_data": {"config": {"mes": mes}},
            "dimensional_star_schema": {"config": {"mes": mes}},
            "business_results_markdown": {"config": {"mes": mes}},
        }
    }


@schedule(
    job=nyc_taxi_pipeline_job,
    cron_schedule="0 8 15 * *",
    description="Ejecuta mensualmente el día 15 a las 08:00 AM el último mes publicado por TLC (rezago 2 meses).",
)
def monthly_tlc_schedule(context: ScheduleEvaluationContext):
    sched_date = context.scheduled_execution_time or datetime.now()
    # La TLC publica con ~2 meses de demora
    target_month = sched_date.month - 2
    target_year = sched_date.year
    if target_month <= 0:
        target_month += 12
        target_year -= 1
    mes_str = f"{target_year}-{target_month:02d}"
    return RunRequest(
        run_key=f"scheduled_{mes_str}",
        run_config=_build_run_config(mes_str),
        tags={"trigger": "monthly_schedule", "mes": mes_str},
    )


@sensor(
    job=nyc_taxi_pipeline_job,
    minimum_interval_seconds=15,
    description="Monitorea data/triggers/*.trigger (ej. 2025-04.trigger) para disparar corridas reactivas.",
)
def new_month_trigger_sensor(context: SensorEvaluationContext):
    TRIGGERS_DIR.mkdir(parents=True, exist_ok=True)
    trigger_files = sorted(TRIGGERS_DIR.glob("*.trigger"))
    if not trigger_files:
        return SkipReason("No hay archivos .trigger pendientes en data/triggers/")

    for tf in trigger_files:
        mes_str = tf.stem.strip()
        tf.unlink()  # Consumir el archivo disparador
        context.log.info(
            f"Trigger detectado para mes '{mes_str}'. Lanzando ejecución..."
        )
        yield RunRequest(
            run_key=f"sensor_{mes_str}_{int(time.time())}",
            run_config=_build_run_config(mes_str),
            tags={"trigger": "file_sensor", "mes": mes_str},
        )


defs = Definitions(
    assets=all_pipeline_assets,
    jobs=[nyc_taxi_pipeline_job],
    schedules=[monthly_tlc_schedule],
    sensors=[new_month_trigger_sensor],
)
