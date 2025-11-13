SELECT company_id, company_name, last_updated_date
FROM {{ source('silver', 'silver_companies') }}