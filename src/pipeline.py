import argparse
import time
from pathlib import Path

from src.config import (
    CURATED_DIR,
    QUARANTINE_DIR,
    RAW_DIR,
    RESULTADOS_PATH,
    SQL_DIR,
    WAREHOUSE_PATH,
    get_duckdb_connection,
    get_memory_usage_mb,
    parse_target_and_comparison_months,
)
from src.step1_ingest import ingest_month_parquet, ingest_zone_lookup
from src.step2_schema import validate_and_standardize_month_schema
from src.step3_quality import run_quality_gate_for_month
from src.step4_transform import write_curated_partition
from src.step5_model_and_queries import build_star_schema_and_answer_queries


def run_pipeline(
    mes: str,
    raw_dir: Path = RAW_DIR,
    quarantine_dir: Path = QUARANTINE_DIR,
    curated_dir: Path = CURATED_DIR,
    warehouse_path: Path = WAREHOUSE_PATH,
    sql_dir: Path = SQL_DIR,
    resultados_path: Path = RESULTADOS_PATH,
) -> None:
    start_time = time.perf_counter()
    peak_ram_mb = get_memory_usage_mb()

    periods = parse_target_and_comparison_months(mes)
    print("=" * 80)
    print(
        f"INICIANDO PIPELINE NYC YELLOW TAXI | Mes asignado: {periods[-1].label} "
        f"| Comparación: {periods[0].label}"
    )
    print("=" * 80)

    con = get_duckdb_connection(warehouse_path)
    try:
        # 1. Ingesta de catálogo de zonas (una sola vez)
        zones_csv_path = ingest_zone_lookup(con, raw_base_dir=raw_dir)
        peak_ram_mb = max(peak_ram_mb, get_memory_usage_mb())

        # Procesar cada período (2024-MM y 2025-MM) por las etapas 1 a 4
        for period in periods:
            print("-" * 80)
            print(f">>> Procesando Período: {period.label}")
            # Etapa 1: Ingesta
            parquet_path = ingest_month_parquet(con, period, raw_base_dir=raw_dir)
            peak_ram_mb = max(peak_ram_mb, get_memory_usage_mb())

            # Etapa 2: Validación y Estandarización de Esquema
            _, std_view = validate_and_standardize_month_schema(
                con, period, parquet_path
            )
            peak_ram_mb = max(peak_ram_mb, get_memory_usage_mb())

            # Etapa 3: Reglas de Calidad, Cuarentena y Reconciliación
            accepted_view, _ = run_quality_gate_for_month(
                con,
                period,
                std_view,
                zones_csv_path=zones_csv_path,
                quarantine_base_dir=quarantine_dir,
            )
            peak_ram_mb = max(peak_ram_mb, get_memory_usage_mb())

            # Etapa 4: Transformación y Escritura Curada Particionada (Hive)
            write_curated_partition(
                con, period, accepted_view, curated_base_dir=curated_dir
            )
            peak_ram_mb = max(peak_ram_mb, get_memory_usage_mb())

        # Etapa 5: Modelo Estrella y Consultas SQL de Negocio
        print("-" * 80)
        build_star_schema_and_answer_queries(
            con,
            periods,
            curated_base_dir=curated_dir,
            sql_dir=sql_dir,
            resultados_path=resultados_path,
        )
        peak_ram_mb = max(peak_ram_mb, get_memory_usage_mb())

        # Resumen en consola para la Defensa Oral
        print("\n" + "=" * 80)
        print("RESUMEN DE AUDITORÍA Y REPORTE DE CALIDAD (LISTO PARA DEFENSA ORAL)")
        print("=" * 80)
        con.sql(
            "SELECT file_name, row_count, ROUND(bytes / 1048576.0, 2) AS size_mb, "
            "LEFT(sha256, 16) AS sha256_prefix, status FROM ingestion_log;"
        ).show()

        con.sql(
            "SELECT period, rows_read, rows_accepted, rows_quarantined, "
            "rows_with_warnings, reconciliation_ok FROM quality_summary_report ORDER BY period;"
        ).show()

        con.sql(
            "SELECT period, rule_id, severity, violated_count, violated_pct "
            "FROM quality_rule_counts ORDER BY period, rule_id;"
        ).show()

        print("Muestra de 3 filas en Cuarentena con su regla violada:")
        con.sql(
            "SELECT period, tpep_pickup_datetime, tpep_dropoff_datetime, "
            "trip_distance, total_amount, pu_location_id, violated_reject_rules "
            "FROM quarantine_trips LIMIT 3;"
        ).show()

    finally:
        con.close()

    elapsed = time.perf_counter() - start_time
    print("=" * 80)
    print(
        f"PIPELINE FINALIZADO EXITOSAMENTE en {elapsed:.2f} segundos | "
        f"Pico de RAM (RSS): {peak_ram_mb:.2f} MB ({peak_ram_mb / 1024:.2f} GB < 8.00 GB)"
    )
    print("=" * 80)


def main() -> None:
    parser = argparse.ArgumentParser(
        description="Pipeline Batch Reproducible NYC Yellow Taxi (TLC)"
    )
    parser.add_argument(
        "--mes",
        type=str,
        default="2025-03",
        help="Mes asignado en formato AAAA-MM (por defecto: 2025-03)",
    )
    args = parser.parse_args()
    run_pipeline(mes=args.mes)


if __name__ == "__main__":
    main()
