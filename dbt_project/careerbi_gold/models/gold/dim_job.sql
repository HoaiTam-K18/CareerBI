WITH silver_jobs AS (
    SELECT
        job_id,
        LOWER(title) AS title_cleaned, 
        job_description,
        job_requirement,
        job_url,
        last_updated_date
    FROM
        {{ source('silver', 'silver_job_postings') }}
)

SELECT
    job_id,
    title_cleaned AS title,
    
    -- === CHUẨN HÓA CHỨC VỤ (LOGIC "SÂU" VÀ "ƯU TIÊN") ===
    CASE
        -- Cấp 1: C-Level / President (Phải chạy TRƯỚC "Director")
        WHEN title_cleaned LIKE '%ceo%' OR title_cleaned LIKE '%chief executive officer%' 
             OR title_cleaned LIKE '%giám đốc điều hành%' THEN 'C-Level'
        WHEN title_cleaned LIKE '%cfo%' OR title_cleaned LIKE '%chief financial officer%' 
             OR title_cleaned LIKE '%giám đốc tài chính%' THEN 'C-Level'
        WHEN title_cleaned LIKE '%cto%' OR title_cleaned LIKE '%chief technology officer%' 
             OR title_cleaned LIKE '%giám đốc công nghệ%' THEN 'C-Level'
        WHEN title_cleaned LIKE '%president%' OR title_cleaned LIKE '%chủ tịch%' THEN 'President'

        -- Cấp 2: Vice President / Director (Phải chạy TRƯỚC "Manager")
        WHEN title_cleaned LIKE '%vice president%' OR title_cleaned LIKE '%phó chủ tịch%' THEN 'Vice President'
        WHEN title_cleaned LIKE '%deputy director%' OR title_cleaned LIKE '%phó giám đốc%' THEN 'Deputy Director'
        WHEN title_cleaned LIKE '%director%' OR title_cleaned LIKE '%giám đốc%' THEN 'Director'

        -- Cấp 3: Manager (Phải chạy TRƯỚC "Lead")
        WHEN title_cleaned LIKE '%deputy manager%' OR title_cleaned LIKE '%phó phòng%' THEN 'Deputy Manager'
        WHEN title_cleaned LIKE '%head of%' THEN 'Manager'
        WHEN title_cleaned LIKE '%manager%' OR title_cleaned LIKE '%trưởng phòng%' 
             OR title_cleaned LIKE '%quản lý%' 
             OR title_cleaned LIKE '%trưởng ban%' THEN 'Manager'

        -- Cấp 4: Lead / Senior (Phải chạy TRƯỚC "Specialist")
        WHEN title_cleaned LIKE '%lead%' OR title_cleaned LIKE '%leader%' 
             OR title_cleaned LIKE '%trưởng nhóm%' THEN 'Lead'
        WHEN title_cleaned LIKE '%senior%' OR title_cleaned LIKE '%cao cấp%' THEN 'Senior'
        
        -- Cấp 5: Specialist / Chuyên viên (Phải chạy TRƯỚC "Staff")
        WHEN title_cleaned LIKE '%specialist%' OR title_cleaned LIKE '%chuyên viên%' THEN 'Specialist'
        WHEN title_cleaned LIKE '%executive%' THEN 'Executive'

        -- (BẮT CÁC NGHỀ CHUYÊN MÔN CAO VÀO "PROFESSIONAL")
        WHEN title_cleaned LIKE '%architect%' OR title_cleaned LIKE '%kiến trúc sư%' 
             OR title_cleaned LIKE '%engineer%' OR title_cleaned LIKE '%kỹ sư%' 
             OR title_cleaned LIKE '%doctor%' OR title_cleaned LIKE '%bác sĩ%' 
             OR title_cleaned LIKE '%lawyer%' OR title_cleaned LIKE '%luật sư%' 
             OR title_cleaned LIKE '%accountant%' OR title_cleaned LIKE '%kế toán%'
             THEN 'Professional' -- (Chuyên gia)

        -- Cấp 6: Junior / Intern (Cấp thấp nhất)
        WHEN title_cleaned LIKE '%junior%' OR title_cleaned LIKE '%mới ra trường%' THEN 'Junior'
        WHEN title_cleaned LIKE '%intern%' OR title_cleaned LIKE '%thực tập%' 
             OR title_cleaned LIKE '%trainee%' THEN 'Intern' -- (Bắt "Management Trainee")

        -- Cấp 7: Mặc định (Ví dụ: "Nhân Viên Kinh Doanh")
        ELSE 'Staff'
    END AS job_level,
    
    job_description,
    job_requirement,
    job_url,
    last_updated_date
FROM
    silver_jobs