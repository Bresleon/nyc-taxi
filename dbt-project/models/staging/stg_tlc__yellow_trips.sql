SELECT
    -- Clave subrogada determinística O(1) RAM (reemplaza ROW_NUMBER() global en memoria)
    MD5(
        CONCAT_WS(
            '|',
            CAST(vendor_id AS VARCHAR),
            CAST(tpep_pickup_datetime AS VARCHAR),
            CAST(tpep_dropoff_datetime AS VARCHAR),
            CAST(pu_location_id AS VARCHAR),
            CAST(do_location_id AS VARCHAR),
            CAST(total_amount AS VARCHAR),
            CAST(trip_distance AS VARCHAR)
        )
    ) AS trip_id,

    -- Claves foráneas y temporales
    CAST(STRFTIME(CAST(tpep_pickup_datetime AS DATE), '%Y%m%d') AS INTEGER) AS pickup_date_key,
    CAST(tpep_pickup_datetime AS DATE) AS pickup_date,
    CAST(EXTRACT('hour' FROM tpep_pickup_datetime) AS INTEGER) AS pickup_hour,
    CAST(tpep_pickup_datetime AS TIMESTAMP) AS pickup_datetime,
    CAST(tpep_dropoff_datetime AS TIMESTAMP) AS dropoff_datetime,
    CAST(pu_location_id AS BIGINT) AS pu_location_id,
    CAST(do_location_id AS BIGINT) AS do_location_id,

    -- Atributos operativos
    CAST(vendor_id AS BIGINT) AS vendor_id,
    CAST(ratecode_id AS DOUBLE) AS ratecode_id,
    TRIM(CAST(store_and_fwd_flag AS VARCHAR)) AS store_and_fwd_flag,
    CAST(payment_type AS BIGINT) AS payment_type,
    CAST(passenger_count AS DOUBLE) AS passenger_count,

    -- Métricas físicas y derivadas
    CAST(trip_distance AS DOUBLE) AS trip_distance,
    CAST(trip_duration_minutes AS DOUBLE) AS trip_duration_minutes,
    CAST(avg_speed_mph AS DOUBLE) AS avg_speed_mph,

    -- Componentes financieros
    CAST(fare_amount AS DOUBLE) AS fare_amount,
    CAST(extra AS DOUBLE) AS extra,
    CAST(mta_tax AS DOUBLE) AS mta_tax,
    CAST(tip_amount AS DOUBLE) AS tip_amount,
    CAST(tip_pct AS DOUBLE) AS tip_pct,
    CAST(tolls_amount AS DOUBLE) AS tolls_amount,
    CAST(improvement_surcharge AS DOUBLE) AS improvement_surcharge,
    CAST(congestion_surcharge AS DOUBLE) AS congestion_surcharge,
    CAST(airport_fee AS DOUBLE) AS airport_fee,
    CAST(cbd_congestion_fee AS DOUBLE) AS cbd_congestion_fee,
    CAST(total_amount AS DOUBLE) AS total_amount,

    -- Banderas de calidad de advertencia desglosadas
    warning_flags,
    LIST_CONTAINS(warning_flags, 'R05_NULL_OR_ZERO_PASSENGERS') AS has_zero_or_null_passengers_warning,
    LIST_CONTAINS(warning_flags, 'R09_EXCESSIVE_PASSENGER_COUNT') AS has_excessive_passengers_warning,

    -- Partición Hive
    CAST(year AS INTEGER) AS year,
    CAST(month AS INTEGER) AS month

FROM {{ source('curated', 'yellow_trips') }}
WHERE CAST(month AS INTEGER) = {{ var('target_month', 3) }}
  AND CAST(year AS INTEGER) IN ({{ var('years', [2024, 2025]) | join(', ') }})
