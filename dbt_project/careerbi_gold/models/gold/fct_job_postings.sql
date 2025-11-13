WITH postings AS (
    SELECT
        job_id,
        company_id,
        salary_max,
        salary_min
    FROM
        {{ source('silver', 'silver_job_postings') }}
),
timelife AS (
    SELECT
        job_id,
        posted_date,
        expiry_date,
        -- Tính toán "Vòng đời"
        CASE 
            WHEN expiry_date IS NOT NULL AND posted_date IS NOT NULL
            THEN DATE_PART('day', expiry_date - posted_date)
            ELSE NULL 
        END AS job_lifespan_days
    FROM
        {{ source('silver', 'silver_job_timelife') }}
),
calendar AS (
    SELECT
        date_key,
        full_date
    FROM
        {{ ref('dim_date') }} -- 'ref' trỏ đến model dbt 'dim_date'
)

SELECT
    -- Khóa (Keys)
    postings.job_id,         -- FK trỏ đến dim_job
    postings.company_id,     -- FK trỏ đến dim_companies
    calendar.date_key AS posted_date_key, -- FK trỏ đến dim_date
    
    -- Số đo (Measures)
    postings.salary_max,
    postings.salary_min,
    timelife.job_lifespan_days

FROM
    postings
LEFT JOIN
    timelife ON postings.job_id = timelife.job_id
LEFT JOIN
    calendar ON timelife.posted_date = calendar.full_date