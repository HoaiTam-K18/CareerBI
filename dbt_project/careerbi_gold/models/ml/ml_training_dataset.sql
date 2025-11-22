-- CTE 1: Skills (M-2-M)
WITH skills_agg AS (
    SELECT
        fs.job_id,
        STRING_AGG(ds.skill_name, ', ') AS all_skills
    FROM {{ ref('fct_job_skills') }} AS fs
    JOIN {{ ref('dim_skills') }} AS ds ON fs.skill_id = ds.skill_id
    GROUP BY 1
),

-- CTE 2: Cities (M-2-M)
cities_agg AS (
    SELECT
        fc.job_id,
        STRING_AGG(dc.city_name, ', ') AS all_cities
    FROM {{ ref('fct_job_cities') }} AS fc
    JOIN {{ ref('dim_cities') }} AS dc ON fc.city_id = dc.city_id
    GROUP BY 1
),

-- CTE 3: Benefits (M-2-M)
benefits_agg AS (
    SELECT
        fb.job_id,
        STRING_AGG(db.benefit_name, ', ') AS all_benefits
    FROM {{ ref('fct_job_benefits') }} AS fb
    JOIN {{ ref('dim_benefits') }} AS db ON fb.benefit_id = db.benefit_id
    GROUP BY 1
),

-- CTE 4: Bảng Fact
fct_posts AS (
    SELECT 
        job_id,
        
        job_function_id,
        group_job_function_id,
        industry_id
        
    FROM {{ ref('fct_job_postings') }}
)

-- =========================================================
-- BƯỚC 3: JOIN NỐI TẤT CẢ
-- =========================================================
SELECT
    p.job_id,
    
    dj.title, 
    dj.job_level,
    
    df.job_function_name,
    dgf.group_job_function_name,
    di.industry_name,
    
    sa.all_skills,
    ca.all_cities,
    ba.all_benefits,
    
    dj.job_description,
    dj.job_requirement
FROM
    fct_posts AS p
LEFT JOIN {{ ref('dim_job') }} AS dj 
    ON p.job_id = dj.job_id
LEFT JOIN {{ ref('dim_job_functions') }} AS df 
    ON p.job_function_id = df.job_function_id
LEFT JOIN {{ ref('dim_group_job_functions') }} AS dgf 
    ON p.group_job_function_id = dgf.group_job_function_id
LEFT JOIN {{ ref('dim_industries') }} AS di
    ON p.industry_id = di.industry_id

LEFT JOIN skills_agg AS sa 
    ON p.job_id = sa.job_id
LEFT JOIN cities_agg AS ca 
    ON p.job_id = ca.job_id
LEFT JOIN benefits_agg AS ba
    ON p.job_id = ba.job_id

WHERE
    dj.job_level IS NOT NULL
    AND sa.all_skills IS NOT NULL
    AND dgf.group_job_function_name IS NOT NULL
    AND dj.job_description IS NOT NULL
    AND ca.all_cities IS NOT NULL
    AND di.industry_name IS NOT NULL
