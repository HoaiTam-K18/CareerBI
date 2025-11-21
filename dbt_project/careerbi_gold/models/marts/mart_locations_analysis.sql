{{ config(materialized='table') }}

WITH dates_per_job AS (
    SELECT 
        fp.job_id,
        dd.calendar_year,
        dd.month_number
    FROM {{ ref('fct_job_postings') }} AS fp
    LEFT JOIN {{ ref('dim_date') }} AS dd ON fp.posted_date_key = dd.date_key
)
SELECT
    -- Dimensions (Chiều)
    d.calendar_year,
    d.month_number,
    c.city_name,
    
    -- Measure (Số đo)
    COUNT(DISTINCT d.job_id) AS job_postings_count

FROM 
    {{ ref('fct_job_cities') }} AS c_bridge
JOIN 
    dates_per_job AS d ON c_bridge.job_id = d.job_id
LEFT JOIN
    {{ ref('dim_cities') }} AS c ON c_bridge.city_id = c.city_id
GROUP BY
    d.calendar_year,
    d.month_number,
    c.city_name