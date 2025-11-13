SELECT city_id, city_name, last_updated_date
FROM {{ source('silver', 'silver_cities') }}