CREATE OR REPLACE TABLE dim_date AS
WITH distinct_dates AS (
    SELECT DISTINCT pickup_date AS full_date
    FROM v_all_curated_trips
    WHERE pickup_date IS NOT NULL
)
SELECT
    CAST(STRFTIME(full_date, '%Y%m%d') AS INTEGER) AS date_key,
    full_date,
    CAST(EXTRACT('year' FROM full_date) AS INTEGER) AS year,
    CAST(EXTRACT('month' FROM full_date) AS INTEGER) AS month,
    MONTHNAME(full_date) AS month_name,
    CAST(EXTRACT('day' FROM full_date) AS INTEGER) AS day_of_month,
    CAST(EXTRACT('isodow' FROM full_date) AS INTEGER) AS day_of_week_num,
    DAYNAME(full_date) AS day_name,
    CASE WHEN EXTRACT('isodow' FROM full_date) IN (6, 7) THEN TRUE ELSE FALSE END AS is_weekend
FROM distinct_dates
ORDER BY full_date;
