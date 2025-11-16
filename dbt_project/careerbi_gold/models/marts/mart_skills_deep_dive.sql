WITH postings AS (
    SELECT job_id, posted_date_key
    FROM {{ ref('fct_job_postings') }}
),
dates AS (
    SELECT date_key, calendar_year, month_number
    FROM {{ ref('dim_date') }}
    WHERE date_key >= 20250801 -- (Lọc "rác")
),
skills AS (
    SELECT fs.job_id, ds.skill_name
    FROM {{ ref('fct_job_skills') }} AS fs
    JOIN {{ ref('dim_skills') }} AS ds ON fs.skill_id = ds.skill_id
),
industries AS (
    SELECT fi.job_id, di.industry_name
    FROM {{ ref('fct_job_industries') }} AS fi
    JOIN {{ ref('dim_industries') }} AS di ON fi.industry_id = di.industry_id
),
functions AS (
    SELECT fg.job_id, dg.group_job_function_name
    FROM {{ ref('fct_job_group_jobfunctions') }} AS fg
    JOIN {{ ref('dim_group_job_functions') }} AS dg ON fg.group_job_function_id = dg.group_job_function_id
),
dim_job AS (
    SELECT job_id, job_level
    FROM {{ ref('dim_job') }}
)
-- JOIN tất cả lại
SELECT
    d.calendar_year,
    d.month_number,
    i.industry_name,
    f.group_job_function_name,
    s.skill_name,
    dj.job_level,
    COUNT(DISTINCT p.job_id) AS job_postings_count
FROM postings AS p
JOIN dates AS d ON p.posted_date_key = d.date_key
JOIN skills AS s ON p.job_id = s.job_id
JOIN industries AS i ON p.job_id = i.job_id
JOIN functions AS f ON p.job_id = f.job_id
JOIN dim_job AS dj ON p.job_id = dj.job_id
GROUP BY 1, 2, 3, 4, 5, 6