import shutil
from pathlib import Path

import duckdb
import pyarrow.parquet as pq

from src.config import CURATED_DIR, MonthPeriod


def write_curated_partition(
    con: duckdb.DuckDBPyConnection,
    period: MonthPeriod,
    accepted_view: str,
    curated_base_dir: Path = CURATED_DIR,
) -> Path:
    """
    Transforma las filas aceptadas agregando columnas derivadas y enriquecimiento
    geográfico, y reemplaza atómicamente solo la partición Hive year=AAAA/month=MM.
    """
    partition_dir = (
        curated_base_dir / f"year={period.year}" / f"month={period.month:02d}"
    )
    tmp_partition_dir = (
        curated_base_dir / f".tmp_year={period.year}_month={period.month:02d}"
    )

    if tmp_partition_dir.exists():
        shutil.rmtree(tmp_partition_dir)
    tmp_partition_dir.mkdir(parents=True, exist_ok=True)

    output_parquet = tmp_partition_dir / f"curated_{period.label}.parquet"

    con.execute(f"""
        COPY (
            SELECT
                a.vendor_id,
                a.tpep_pickup_datetime,
                a.tpep_dropoff_datetime,
                CAST(a.tpep_pickup_datetime AS DATE) AS pickup_date,
                CAST(EXTRACT('hour' FROM a.tpep_pickup_datetime) AS INTEGER) AS pickup_hour,
                DAYNAME(a.tpep_pickup_datetime) AS pickup_day_of_week,
                CAST(EXTRACT('isodow' FROM a.tpep_pickup_datetime) AS INTEGER) AS pickup_day_of_week_num,
                ROUND(
                    EPOCH(a.tpep_dropoff_datetime - a.tpep_pickup_datetime) / 60.0,
                    2
                ) AS trip_duration_minutes,
                ROUND(
                    a.trip_distance / NULLIF(
                        EPOCH(a.tpep_dropoff_datetime - a.tpep_pickup_datetime) / 3600.0,
                        0.0
                    ),
                    2
                ) AS avg_speed_mph,
                a.passenger_count,
                a.trip_distance,
                a.ratecode_id,
                a.store_and_fwd_flag,
                a.pu_location_id,
                pz.borough AS pu_borough,
                pz.zone_name AS pu_zone,
                a.do_location_id,
                dz.borough AS do_borough,
                dz.zone_name AS do_zone,
                a.payment_type,
                a.fare_amount,
                a.extra,
                a.mta_tax,
                a.tip_amount,
                ROUND(
                    CASE
                        WHEN a.fare_amount > 0.0 THEN (a.tip_amount / a.fare_amount) * 100.0
                        ELSE 0.0
                    END,
                    2
                ) AS tip_pct,
                a.tolls_amount,
                a.improvement_surcharge,
                a.congestion_surcharge,
                a.airport_fee,
                a.cbd_congestion_fee,
                a.total_amount,
                a.warning_flags,
                {period.year} AS year,
                {period.month} AS month
            FROM {accepted_view} a
            LEFT JOIN raw_taxi_zones pz ON a.pu_location_id = pz.location_id
            LEFT JOIN raw_taxi_zones dz ON a.do_location_id = dz.location_id
        ) TO '{output_parquet.as_posix()}' (FORMAT PARQUET, COMPRESSION ZSTD);
    """)

    # Reemplazo atómico de la partición del mes sin tocar las demás particiones
    partition_dir.parent.mkdir(parents=True, exist_ok=True)
    if partition_dir.exists():
        shutil.rmtree(partition_dir)
    shutil.move(str(tmp_partition_dir), str(partition_dir))

    final_parquet = partition_dir / f"curated_{period.label}.parquet"
    written_rows = pq.read_metadata(final_parquet).num_rows
    print(
        f"[CURADA {period.label}] Partición Hive actualizada: "
        f"{partition_dir.relative_to(curated_base_dir.parent.parent)} ({written_rows:,} filas)"
    )
    return final_parquet
