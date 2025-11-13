SELECT
    job_id,
    title,
    job_description,
    job_requirement,
    job_url,
    last_updated_date
FROM
    {{ source('silver', 'silver_job_postings') }}