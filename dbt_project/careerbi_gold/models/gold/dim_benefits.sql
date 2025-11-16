SELECT 
    benefit_id, 
    benefit_name, 
    benefit_value, 
    last_updated_date
FROM 
    {{ source('silver', 'silver_benefits') }}