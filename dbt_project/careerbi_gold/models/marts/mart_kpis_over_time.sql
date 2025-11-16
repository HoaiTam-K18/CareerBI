SELECT
    d.calendar_year,
    d.month_number,
    COUNT(DISTINCT f.job_id) AS job_postings_count
FROM
    {{ ref('fct_job_postings') }} AS f
JOIN
    {{ ref('dim_date') }} AS d
ON f.posted_date_key = d.date_key
WHERE d.date_key >= 20250801 -- (Lọc "rác")
GROUP BY 1, 2
ORDER BY 1, d.month_number