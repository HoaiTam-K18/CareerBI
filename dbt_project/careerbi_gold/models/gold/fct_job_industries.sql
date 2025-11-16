SELECT
    job_id,
    industry_id
FROM
    {{ source('silver', 'silver_bridge_job_industries') }}