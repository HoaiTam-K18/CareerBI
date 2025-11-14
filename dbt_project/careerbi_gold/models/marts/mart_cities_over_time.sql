-- models/marts/mart_cities_over_time.sql
-- "Nấu" (pre-calculate) số lượng job cho MỌI thành phố, MỌI tháng

WITH dates_per_job AS (
    SELECT 
        fp.job_id,
        dd.calendar_year,
        dd.month_name
    FROM {{ ref('fct_job_postings') }} AS fp
    LEFT JOIN {{ ref('dim_date') }} AS dd ON fp.posted_date_key = dd.date_key
)
SELECT
    -- Dimensions (Chiều)
    d.calendar_year,
    d.month_name,
    c.city_name,
    
    -- Measure (Số đo)
    -- SỬA LẠI: 'c.job_id' -> 'd.job_id' (hoặc 'c_bridge.job_id')
    COUNT(DISTINCT d.job_id) AS so_luong_tin_tuyen_dung

FROM 
    {{ ref('fct_job_cities') }} AS c_bridge
JOIN 
    dates_per_job AS d ON c_bridge.job_id = d.job_id
LEFT JOIN
    {{ ref('dim_cities') }} AS c ON c_bridge.city_id = c.city_id
GROUP BY
    d.calendar_year,
    d.month_name,
    c.city_name