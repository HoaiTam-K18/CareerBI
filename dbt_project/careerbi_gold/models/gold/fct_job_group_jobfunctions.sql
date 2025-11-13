SELECT job_id, group_job_function_id
FROM {{ source('silver', 'silver_bridge_job_group_jobfunctions') }}