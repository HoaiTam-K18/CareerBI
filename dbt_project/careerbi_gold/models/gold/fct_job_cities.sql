SELECT job_id, city_id
FROM {{ source('silver', 'silver_bridge_job_cities') }}