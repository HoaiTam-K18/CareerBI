{% macro analyze_kpis_by_dimension(dim_ref, bridge_ref, dim_pk, dim_name_col) %}
{# Macro này "nấu" (pre-calculate) KPIs theo 1 chiều (Industry/Function) VÀ 'job_level' #}

WITH postings AS (
    -- NỐI (JOIN) VỚI dim_job NGAY TẠI ĐÂY
    SELECT
        f.job_id,
        f.posted_date_key,
        f.salary_max,
        f.job_lifespan_days,
        d.job_level -- <-- KÉO (PULL) 'job_level' VÀO
    FROM {{ ref('fct_job_postings') }} AS f
    LEFT JOIN {{ ref('dim_job') }} AS d ON f.job_id = d.job_id
),
dates AS (
    SELECT date_key, calendar_year, month_number
    FROM {{ ref('dim_date') }}
    WHERE date_key >= 20250801 -- (Lọc "rác")
),
bridge AS (
    SELECT job_id, {{ dim_pk }}
    FROM {{ bridge_ref }}
),
dim AS (
    SELECT {{ dim_pk }}, {{ dim_name_col }}
    FROM {{ dim_ref }}
)
-- SỬA LẠI SELECT CUỐI CÙNG
SELECT
    d.calendar_year,
    d.month_number,
    di.{{ dim_name_col }},
    p.job_level,
    
    COUNT(DISTINCT p.job_id) AS job_postings_count,
    AVG(p.salary_max) AS avg_salary_max,
    AVG(p.job_lifespan_days) AS avg_lifespan_days

FROM postings AS p
JOIN dates AS d ON p.posted_date_key = d.date_key
JOIN bridge AS b ON p.job_id = b.job_id
JOIN dim AS di ON b.{{ dim_pk }} = di.{{ dim_pk }}
GROUP BY 1, 2, 3, 4

{% endmacro %}