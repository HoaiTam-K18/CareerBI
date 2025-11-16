SELECT 
    group_job_function_id,
    group_job_function_name,
    last_updated_date
FROM 
    {{ source('silver', 'silver_group_job_functions') }}