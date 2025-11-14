-- "Nấu" (pre-calculate) các KPI chính theo thời gian

SELECT
    -- Dimensions (Chiều)
    d.calendar_year,
    d.calendar_quarter,
    d.month_number,
    d.month_name,
    
    -- Measures (Số đo)
    COUNT(DISTINCT f.job_id) AS so_luong_tin_dang,
    AVG(f.salary_max) AS luong_trung_binh_max,
    AVG(f.job_lifespan_days) AS vong_doi_trung_binh

FROM
    {{ ref('fct_job_postings') }} AS f
LEFT JOIN
    {{ ref('dim_date') }} AS d ON f.posted_date_key = d.date_key
GROUP BY
    d.calendar_year,
    d.calendar_quarter,
    d.month_number,
    d.month_name