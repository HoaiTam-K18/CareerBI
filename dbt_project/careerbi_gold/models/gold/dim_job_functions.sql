SELECT 
    job_function_id,
    job_function_name,
    last_updated_date
FROM
    {{ source('silver', 'silver_job_functions') }}