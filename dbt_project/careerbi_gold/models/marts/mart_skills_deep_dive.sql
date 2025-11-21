-- marts/skill_analysis.sql
-- (Phiên bản TỐI ƯU HÓA - Phân tích theo CHỨC NĂNG CÔNG VIỆC)

WITH postings AS (
    SELECT 
        job_id, 
        posted_date_key,
        -- Giữ lại 2 khóa 1:1 mới cho chức năng công việc
        job_function_id, 
        group_job_function_id 
    FROM {{ ref('fct_job_postings') }}
),

-- Chiều Thời gian
dates AS (
    SELECT date_key, calendar_year, month_number
    FROM {{ ref('dim_date') }}
    WHERE date_key >= 20250901
),

-- Chiều Cấp bậc
dim_job AS (
    SELECT job_id, job_level
    FROM {{ ref('dim_job') }} 
),

-- Chiều M-2-M (Kỹ năng)
skills AS (
    SELECT fs.job_id, ds.skill_name
    FROM {{ ref('fct_job_skills') }} AS fs
    JOIN {{ ref('dim_skills') }} AS ds ON fs.skill_id = ds.skill_id
),

-- Chiều Chức năng Công việc (1:1 mới)
dim_functions AS (
    SELECT job_function_id, job_function_name
    FROM {{ ref('dim_job_functions') }}
),
dim_group_functions AS (
    SELECT group_job_function_id, group_job_function_name
    FROM {{ ref('dim_group_job_functions') }}
)


-- === KHỐI SELECT CUỐI CÙNG ===
SELECT
    -- Chiều (Dimensions)
    d.calendar_year,
    d.month_number,
    dgf.group_job_function_name,
    df.job_function_name,
    s.skill_name,
    dj.job_level,
    
    -- Số đo (Measures)
    COUNT(DISTINCT p.job_id) AS job_postings_count
    
FROM postings AS p
JOIN dates AS d ON p.posted_date_key = d.date_key
JOIN skills AS s ON p.job_id = s.job_id
JOIN dim_job AS dj ON p.job_id = dj.job_id

-- Nối 2 cấp chức năng công việc (Job Functions)
JOIN dim_functions AS df ON p.job_function_id = df.job_function_id
JOIN dim_group_functions AS dgf ON p.group_job_function_id = dgf.group_job_function_id

GROUP BY 1, 2, 3, 4, 5, 6