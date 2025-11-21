{{ config(materialized='table') }}

SELECT
    d.benefit_name,
    COUNT(DISTINCT f.job_id) AS job_postings_count
FROM
    {{ ref('fct_job_benefits') }} AS f
JOIN
    {{ ref('dim_benefits') }} AS d
ON f.benefit_id = d.benefit_id
GROUP BY
    d.benefit_name
ORDER BY
    job_postings_count DESC