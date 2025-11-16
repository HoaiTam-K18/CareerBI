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
        {{ ref('dim_date') }}
)

-- === KHỐI SELECT CUỐI CÙNG ===
SELECT
    -- Khóa (Keys)
    postings.job_id,
    postings.company_id,
    calendar.date_key AS posted_date_key,
    
    -- Số đo (Measures) - ĐÃ SỬA THEO YÊU CẦU MỚI
    
    -- (Bất cứ lương MAX nào dưới $150 -> NULL)
    CASE 
        WHEN postings.salary_max < 150 THEN NULL
        ELSE postings.salary_max 
    END AS salary_max,
    
    CASE 
        WHEN postings.salary_min < 250 THEN NULL
        ELSE postings.salary_min
    END AS salary_min,

    -- (Bất cứ tin nào > 90 ngày -> NULL)
    CASE 
        WHEN timelife.job_lifespan_days > 90 THEN NULL
        ELSE timelife.job_lifespan_days
    END AS job_lifespan_days

FROM
    postings
LEFT JOIN
    timelife ON postings.job_id = timelife.job_id
LEFT JOIN
    calendar ON timelife.posted_date = calendar.full_date