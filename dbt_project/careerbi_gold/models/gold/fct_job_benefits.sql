SELECT 
    job_id,
    benefit_id
FROM
    {{ source('silver', 'silver_bridge_job_benefits') }}