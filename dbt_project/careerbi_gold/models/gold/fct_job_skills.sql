SELECT
    job_id,
    skill_id
FROM
    {{ source('silver', 'silver_bridge_job_skills') }}