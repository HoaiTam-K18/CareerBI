WITH date_spine AS (
  {{ dbt_utils.date_spine(
      datepart="day",
      start_date="cast('2023-01-01' as date)",
      end_date="cast('2026-12-31' as date)"
     )
  }}
)

SELECT
    CAST(TO_CHAR(date_day, 'YYYYMMDD') AS INT) AS date_key,
    date_day AS full_date,
    EXTRACT(YEAR FROM date_day) AS calendar_year,
    EXTRACT(QUARTER FROM date_day) AS calendar_quarter,
    EXTRACT(MONTH FROM date_day) AS month_number,
    TO_CHAR(date_day, 'Month') AS month_name,
    EXTRACT(ISODOW FROM date_day) AS day_number_of_week,
    TO_CHAR(date_day, 'Day') AS day_name,
    (EXTRACT(ISODOW FROM date_day) IN (6, 7)) AS is_weekend
FROM
    date_spine