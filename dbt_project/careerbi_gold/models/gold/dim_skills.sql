SELECT skill_id, skill_name, last_updated_date
FROM {{ source('silver', 'silver_skills') }}