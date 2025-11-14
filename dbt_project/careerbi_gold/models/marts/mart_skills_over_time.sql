-- models/marts/mart_skills_over_time.sql
-- "Nấu" (pre-calculate) số lượng job cho MỌI skill, MỌI tháng

WITH dates_per_job AS (
    -- Lấy Năm/Tháng cho mỗi Job
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
    s.skill_name,
    
    -- Measure (Số đo)
    -- SỬA LẠI: 's.job_id' -> 'd.job_id' (hoặc 's_bridge.job_id')
    COUNT(DISTINCT d.job_id) AS so_luong_tin_tuyen_dung

FROM 
    {{ ref('fct_job_skills') }} AS s_bridge
JOIN 
    dates_per_job AS d ON s_bridge.job_id = d.job_id
LEFT JOIN
    {{ ref('dim_skills') }} AS s ON s_bridge.skill_id = s.skill_id
GROUP BY
    d.calendar_year,
    d.month_name,
    s.skill_name