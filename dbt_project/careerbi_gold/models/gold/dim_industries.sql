SELECT 
    industry_id,
    industry_name,
    last_updated_date
FROM 
    {{ source('silver', 'silver_industries') }}