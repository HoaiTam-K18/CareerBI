{% macro analyze_kpis_by_dimension(dim_ref, dim_pk, dim_name_col, is_direct_join=False, bridge_ref=None) %}

WITH postings AS (
    SELECT
        f.job_id,
        f.posted_date_key,
        f.salary_max,
        f.job_lifespan_days,
        d.job_level
        
        {% if is_direct_join %}
            , f.{{ dim_pk }}
        {% endif %}
        
    FROM {{ ref('fct_job_postings') }} AS f
    LEFT JOIN {{ ref('dim_job') }} AS d ON f.job_id = d.job_id
),
dates AS (
    SELECT date_key, calendar_year, month_number
    FROM {{ ref('dim_date') }}
    WHERE date_key >= 20250901
),
dim AS (
    SELECT {{ dim_pk }}, {{ dim_name_col }}
    FROM {{ dim_ref }}
)

{% if not is_direct_join %}
, bridge AS (
    SELECT job_id, {{ dim_pk }}
    FROM {{ bridge_ref }}
)
{% endif %}

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

{% if is_direct_join %}
    JOIN dim AS di ON p.{{ dim_pk }} = di.{{ dim_pk }}
    
{% else %}
    JOIN bridge AS b ON p.job_id = b.job_id
    JOIN dim AS di ON b.{{ dim_pk }} = di.{{ dim_pk }}
{% endif %}
{# (FIX: "Xóa" (Delete) "logic" (logic) JOIN "lỗi" (buggy) "cũ" (old)) #}

GROUP BY 1, 2, 3, 4

{% endmacro %}