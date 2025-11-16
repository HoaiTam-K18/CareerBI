SELECT
    job_id,
    job_function_id
FROM
    {{ source('silver', 'silver_bridge_job_jobfunctions') }}