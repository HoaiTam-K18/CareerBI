import pandas as pd
import re
from .db_utils import get_db_engine # Import từ file chung

def clean_text_data(raw_text):
    """
    Hàm dọn dẹp HTML, ký tự đặc biệt, và khoảng trắng thừa.
    """
    if not isinstance(raw_text, str):
        return ""
    clean_text = re.sub(r'<[^>]+>', ' ', str(raw_text))
    clean_text = re.sub(r'[^\w\s]', ' ', clean_text)
    clean_text = re.sub(r'[\n\t\r]+', ' ', clean_text)
    clean_text = re.sub(r'\s+', ' ', clean_text).strip()
    return clean_text.lower()

# ===================================================
# LOGIC TASK 3: TRANSFORM SILVER
# ===================================================

def transform_silver():
    """ 
    Đọc từ Bronze -> Dọn dẹp/Lọc (bằng Pandas) -> Tải vào Silver.
    (Phiên bản cuối: Sửa lỗi Transaction, NotNull, PK, và IF EXISTS)
    """
    print("Bắt đầu tác vụ Transform (T) lên Silver Layer...")
    bronze_schema = "bronze"
    silver_schema = "silver"
    VND_TO_USD_RATE = 25000 
    
    try:
        with get_db_engine() as engine:
            
            # 1. Mở Transaction, xóa sạch schema cũ
            with engine.begin() as conn: 
                print(f"Đang xóa schema '{silver_schema}' cũ (nếu tồn tại) bằng CASCADE...")
                conn.execute(f"DROP SCHEMA IF EXISTS {silver_schema} CASCADE;")
                
                print(f"Đang tạo schema '{silver_schema}' mới...")
                conn.execute(f"CREATE SCHEMA {silver_schema};")
                
            # (Transaction này đóng lại)

            # ================================================
            # PHẦN 1: TẢI DỮ LIỆU (LOAD DATA)
            # (Dùng 'engine' (pool) để tránh timeout)
            # ================================================

            # --- Bảng 1: silver_companies ---
            print("Đang xử lý silver.silver_companies...")
            df = pd.read_sql_table("bronze_companies", engine, schema=bronze_schema, columns=['companyId', 'companyName', 'load_date'])
            df = df.sort_values(by='load_date').drop_duplicates(subset=['companyId'], keep='last')
            df = df.rename(columns={'companyId': 'company_id', 'companyName': 'company_name', 'load_date': 'last_updated_date'})
            df['company_id'] = pd.to_numeric(df['company_id'], errors='coerce').astype('Int64')
            df = df.dropna(subset=['company_id'])
            df_silver = df[['company_id', 'company_name', 'last_updated_date']].set_index('company_id')
            df_silver.to_sql("silver_companies", engine, schema=silver_schema, if_exists='replace', index=True, index_label='company_id')

            # --- Bảng 2: silver_cities ---
            print("Đang xử lý silver.silver_cities...")
            df = pd.read_sql_table("bronze_cities", engine, schema=bronze_schema, columns=['cityId', 'cityName', 'load_date'])
            df = df.sort_values(by='load_date').drop_duplicates(subset=['cityId'], keep='last')
            df = df.rename(columns={'cityId': 'city_id', 'cityName': 'city_name', 'load_date': 'last_updated_date'})
            df['city_id'] = pd.to_numeric(df['city_id'], errors='coerce').astype('Int64')
            df = df.dropna(subset=['city_id'])
            df_silver = df[['city_id', 'city_name', 'last_updated_date']].set_index('city_id')
            df_silver.to_sql("silver_cities", engine, schema=silver_schema, if_exists='replace', index=True, index_label='city_id')

            # --- Bảng 3: silver_skills ---
            print("Đang xử lý silver.silver_skills...")
            df = pd.read_sql_table("bronze_skills", engine, schema=bronze_schema, columns=['skillId', 'skillName', 'load_date'])
            df = df.sort_values(by='load_date').drop_duplicates(subset=['skillId'], keep='last')
            df = df.rename(columns={'skillId': 'skill_id', 'skillName': 'skill_name', 'load_date': 'last_updated_date'})
            df['skill_id'] = pd.to_numeric(df['skill_id'], errors='coerce').astype('Int64')
            df = df.dropna(subset=['skill_id'])
            df['skill_name'] = df['skill_name'].apply(clean_text_data)
            df_silver = df[['skill_id', 'skill_name', 'last_updated_date']].set_index('skill_id')
            df_silver.to_sql("silver_skills", engine, schema=silver_schema, if_exists='replace', index=True, index_label='skill_id')

            # --- Bảng 4: silver_benefits ---
            print("Đang xử lý silver.silver_benefits...")
            df = pd.read_sql_table("bronze_benefits", engine, schema=bronze_schema, columns=['benefitId', 'benefitName', 'benefitValue', 'load_date'])
            df = df.sort_values(by='load_date').drop_duplicates(subset=['benefitId'], keep='last')
            df = df.rename(columns={'benefitId': 'benefit_id', 'benefitName': 'benefit_name', 'benefitValue': 'benefit_value', 'load_date': 'last_updated_date'})
            df['benefit_id'] = pd.to_numeric(df['benefit_id'], errors='coerce').astype('Int64')
            df['benefit_value'] = df['benefit_value'].apply(clean_text_data)
            df = df.dropna(subset=['benefit_id'])
            df_silver = df[['benefit_id', 'benefit_name', 'benefit_value', 'last_updated_date']].set_index('benefit_id')
            df_silver.to_sql("silver_benefits", engine, schema=silver_schema, if_exists='replace', index=True, index_label='benefit_id')

            # --- Bảng 5: silver_industries ---
            print("Đang xử lý silver.silver_industries...")
            df = pd.read_sql_table("bronze_industries", engine, schema=bronze_schema, columns=['industryId', 'industryName', 'load_date'])
            df = df.sort_values(by='load_date').drop_duplicates(subset=['industryId'], keep='last')
            df = df.rename(columns={'industryId': 'industry_id', 'industryName': 'industry_name', 'load_date': 'last_updated_date'})
            df['industry_id'] = pd.to_numeric(df['industry_id'], errors='coerce').astype('Int64')
            df = df.dropna(subset=['industry_id'])
            df_silver = df[['industry_id', 'industry_name', 'last_updated_date']].set_index('industry_id')
            df_silver.to_sql("silver_industries", engine, schema=silver_schema, if_exists='replace', index=True, index_label='industry_id')

            # --- Bảng 6: silver_job_functions ---
            print("Đang xử lý silver.silver_job_functions...")
            df = pd.read_sql_table("bronze_job_functions", engine, schema=bronze_schema, columns=['jobFunctionId', 'jobFunctionName', 'load_date'])
            df = df.sort_values(by='load_date').drop_duplicates(subset=['jobFunctionId'], keep='last')
            df = df.rename(columns={'jobFunctionId': 'job_function_id', 'jobFunctionName': 'job_function_name', 'load_date': 'last_updated_date'})
            df['job_function_id'] = pd.to_numeric(df['job_function_id'], errors='coerce').astype('Int64')
            df = df.dropna(subset=['job_function_id'])
            df_silver = df[['job_function_id', 'job_function_name', 'last_updated_date']].set_index('job_function_id')
            df_silver.to_sql("silver_job_functions", engine, schema=silver_schema, if_exists='replace', index=True, index_label='job_function_id')

            # --- Bảng 7: silver_group_job_functions ---
            print("Đang xử lý silver.silver_group_job_functions...")
            df = pd.read_sql_table("bronze_group_job_functions", engine, schema=bronze_schema, columns=['groupJobFunctionId', 'groupJobFunctionName', 'load_date'])
            df = df.sort_values(by='load_date').drop_duplicates(subset=['groupJobFunctionId'], keep='last')
            df = df.rename(columns={'groupJobFunctionId': 'group_job_function_id', 'groupJobFunctionName': 'group_job_function_name', 'load_date': 'last_updated_date'})
            df['group_job_function_id'] = pd.to_numeric(df['group_job_function_id'], errors='coerce').astype('Int64')
            df = df.dropna(subset=['group_job_function_id'])
            df_silver = df[['group_job_function_id', 'group_job_function_name', 'last_updated_date']].set_index('group_job_function_id')
            df_silver.to_sql("silver_group_job_functions", engine, schema=silver_schema, if_exists='replace', index=True, index_label='group_job_function_id')

            # FACT_TABLE
            
            # --- Bảng 8: silver_job_postings (Bảng "Cha") ---
            print("Đang xử lý silver.silver_job_postings (Bảng Cha)...")
            df = pd.read_sql_table("bronze_job_postings", engine, schema=bronze_schema)
            df = df.sort_values(by='load_date').drop_duplicates(subset=['job_id'], keep='last')
            df = df.rename(columns={'title': 'title', 'jobDescription': 'job_description', 'jobRequirement': 'job_requirement', 'salaryMax': 'salary_max', 'salaryMin': 'salary_min', 'salaryCurrency': 'salary_currency', 'companyId': 'company_id', 'job_url': 'job_url', 'load_date': 'last_updated_date'})
            df['job_description'] = df['job_description'].apply(clean_text_data)
            df['job_requirement'] = df['job_requirement'].apply(clean_text_data)
            df['job_id'] = pd.to_numeric(df['job_id'], errors='coerce').astype('Int64')
            df['company_id'] = pd.to_numeric(df['company_id'], errors='coerce').astype('Int64')
            df = df.dropna(subset=['job_id']) # Lọc 'job_id' bị NULL
            df['salary_max'] = pd.to_numeric(df['salary_max'], errors='coerce').fillna(0).astype(float)
            df['salary_min'] = pd.to_numeric(df['salary_min'], errors='coerce').fillna(0).astype(float)
            vnd_rows = df['salary_currency'].str.upper() == 'VND'
            df.loc[vnd_rows, 'salary_max'] = df.loc[vnd_rows, 'salary_max'] / VND_TO_USD_RATE
            df.loc[vnd_rows, 'salary_min'] = df.loc[vnd_rows, 'salary_min'] / VND_TO_USD_RATE
            df.loc[vnd_rows, 'salary_currency'] = 'USD'
            df_silver = df[['job_id', 'title', 'job_description', 'job_requirement', 'salary_max', 'salary_min', 'salary_currency', 'company_id', 'job_url', 'last_updated_date']]
            df_silver = df_silver.set_index('job_id')
            
            # <<< SỬA LỖI 1: Lấy danh sách ID "cha" HỢP LỆ >>>
            valid_job_ids = df_silver.index
            print(f"Đã xác định được {len(valid_job_ids)} job_id 'cha' hợp lệ.")
            
            df_silver.to_sql("silver_job_postings", engine, schema=silver_schema, if_exists='replace', index=True, index_label='job_id')

            # --- Bảng 9: silver_job_timelife (Bảng "Con") ---
            print("Đang xử lý silver.silver_job_timelife (Bảng Con)...")
            df = pd.read_sql_table("bronze_job_timelife", engine, schema=bronze_schema, columns=['job_id', 'createdOnDateKey', 'expiredOnDateKey', 'load_date'])
            df = df.sort_values(by='load_date').drop_duplicates(subset=['job_id'], keep='last')
            df = df.rename(columns={'createdOnDateKey': 'posted_date', 'expiredOnDateKey': 'expiry_date', 'load_date': 'last_updated_date'})
            df['job_id'] = pd.to_numeric(df['job_id'], errors='coerce').astype('Int64')
            df = df.dropna(subset=['job_id'])
            
            df = df[df['job_id'].isin(valid_job_ids)]
            print(f"  -> Đã lọc, còn {len(df)} hàng 'timelife' hợp lệ.")
            
            df['posted_date'] = pd.to_datetime(df['posted_date'], format='%Y%m%d', errors='coerce')
            df['expiry_date'] = pd.to_datetime(df['expiry_date'], format='%Y%m%d', errors='coerce')
            df_silver = df[['job_id', 'posted_date', 'expiry_date', 'last_updated_date']].set_index('job_id')
            df_silver.to_sql("silver_job_timelife", engine, schema=silver_schema, if_exists='replace', index=True, index_label='job_id')

            # --- Bảng 10 - 15 (Các bảng Bridge - Bảng "Con") ---
            print("Đang xử lý các bảng Bridge (Bảng Con)...")
            bridge_config = [
                ("bronze_bridge_job_skills", "silver_bridge_job_skills", "jobId", "job_id", "skillId", "skill_id"),
                ("bronze_bridge_job_benefits", "silver_bridge_job_benefits", "jobId", "job_id", "benefitId", "benefit_id"),
                ("bronze_bridge_job_industries", "silver_bridge_job_industries", "jobId", "job_id", "industryId", "industry_id"),
                ("bronze_bridge_job_cities", "silver_bridge_job_cities", "jobId", "job_id", "cityId", "city_id"),
                ("bronze_bridge_job_jobFunctions", "silver_bridge_job_jobfunctions", "jobId", "job_id", "jobFunctionId", "job_function_id"),
                ("bronze_bridge_job_group_jobFunctions", "silver_bridge_job_group_jobfunctions", "jobId", "job_id", "groupJobFunctionId", "group_job_function_id")
            ]
            for b_table, s_table, b_col1, s_col1, b_col2, s_col2 in bridge_config:
                print(f"  -> Đang xử lý {s_table} (từ {b_table})")
                df_bridge = pd.read_sql_table(b_table, engine, schema=bronze_schema, columns=[b_col1, b_col2, 'load_date'])
                df_bridge = df_bridge.sort_values(by='load_date').drop_duplicates(subset=[b_col1, b_col2], keep='last')
                df_bridge = df_bridge.rename(columns={b_col1: s_col1, b_col2: s_col2})
                df_bridge[s_col1] = pd.to_numeric(df_bridge[s_col1], errors='coerce').astype('Int64')
                df_bridge[s_col2] = pd.to_numeric(df_bridge[s_col2], errors='coerce').astype('Int64')
                
                df_bridge = df_bridge[df_bridge[s_col1].isin(valid_job_ids)] # s_col1 là 'job_id'
                print(f"    -> Đã lọc, còn {len(df_bridge)} hàng bridge hợp lệ.")

                df_silver_bridge = df_bridge[[s_col1, s_col2]]
                df_silver_bridge.to_sql(s_table, engine, schema=silver_schema, if_exists='replace', index=False)

            # ================================================
            # BƯỚC 4 (PHẦN 2): TẠO LIÊN KẾT
            # ================================================
            
            print("Tất cả bảng đã tải. Mở kết nối MỚI để tạo liên kết...")
            with engine.connect() as conn:
                
                # --- 2.1: Tạo Index (Chỉ mục) ---
                index_config = [
                    ('silver.silver_job_postings', 'idx_jobs_company_id', 'company_id'),
                    ('silver.silver_bridge_job_skills', 'idx_bridge_skill_id', 'skill_id'),
                    ('silver.silver_bridge_job_benefits', 'idx_bridge_benefit_id', 'benefit_id'),
                    ('silver.silver_bridge_job_industries', 'idx_bridge_industry_id', 'industry_id'),
                    ('silver.silver_bridge_job_cities', 'idx_bridge_city_id', 'city_id'),
                    ('silver.silver_bridge_job_jobfunctions', 'idx_bridge_function_id', 'job_function_id'),
                    ('silver.silver_bridge_job_group_jobfunctions', 'idx_bridge_group_func_id', 'group_job_function_id')
                ]
                for table, idx_name, col in index_config:
                    print(f"Tạo Index: {idx_name} trên {table}({col})")
                    try:
                        conn.execute(f"CREATE INDEX {idx_name} ON {table} ({col});")
                    except Exception as e:
                        if "does not exist" in str(e):
                            print(f"CẢNH BÁO: Bảng {table} không tồn tại (do web sập). Bỏ qua tạo Index.")
                        else:
                            raise e

                # --- 2.2: Tạo Khóa Chính (PK) ---
                print("Đang tạo các Khóa Chính (PK)...")
                pk_config = [
                    ('silver.silver_job_postings', 'pk_job_postings', ('job_id',)),
                    ('silver.silver_job_timelife', 'pk_job_timelife', ('job_id',)),
                    ('silver.silver_companies', 'pk_companies', ('company_id',)),
                    ('silver.silver_cities', 'pk_cities', ('city_id',)),
                    ('silver.silver_skills', 'pk_skills', ('skill_id',)),
                    ('silver.silver_benefits', 'pk_benefits', ('benefit_id',)),
                    ('silver.silver_industries', 'pk_industries', ('industry_id',)),
                    ('silver.silver_job_functions', 'pk_job_functions', ('job_function_id',)),
                    ('silver.silver_group_job_functions', 'pk_group_job_functions', ('group_job_function_id',)),
                    ('silver.silver_bridge_job_skills', 'pk_bridge_skills', ('job_id', 'skill_id')),
                    ('silver.silver_bridge_job_benefits', 'pk_bridge_benefits', ('job_id', 'benefit_id')),
                    ('silver.silver_bridge_job_industries', 'pk_bridge_industries', ('job_id', 'industry_id')),
                    ('silver.silver_bridge_job_cities', 'pk_bridge_cities', ('job_id', 'city_id')),
                    ('silver.silver_bridge_job_jobfunctions', 'pk_bridge_jobfunctions', ('job_id', 'job_function_id')),
                    ('silver.silver_bridge_job_group_jobfunctions', 'pk_bridge_groupjobfunctions', ('job_id', 'group_job_function_id'))
                ]
                
                for table, pk_name, cols in pk_config:
                    cols_str = ', '.join(cols)
                    print(f"Tạo PK: {pk_name} trên {table}({cols_str})")
                    try:
                        conn.execute(f"""
                            ALTER TABLE {table}
                            ADD CONSTRAINT {pk_name}
                            PRIMARY KEY ({cols_str});
                        """)
                    except Exception as e:
                        if "does not exist" in str(e):
                            print(f"CẢNH BÁO: Bảng {table} không tồn tại (do web sập). Bỏ qua tạo PK.")
                        else:
                            raise e

                # --- 2.3: Tạo Khóa Ngoại (FK) ---
                print("Đang tạo các Khóa Ngoại (FK)...")
                fk_config = [
                    ('silver.silver_job_postings', 'company_id', 'fk_jobs_company', 'silver.silver_companies', 'company_id'),
                    ('silver.silver_job_timelife', 'job_id', 'fk_jobs_timelife', 'silver.silver_job_postings', 'job_id'),
                    ('silver.silver_bridge_job_skills', 'job_id', 'fk_skill_job', 'silver.silver_job_postings', 'job_id'),
                    ('silver.silver_bridge_job_skills', 'skill_id', 'fk_skill_dim', 'silver.silver_skills', 'skill_id'),
                    ('silver.silver_bridge_job_benefits', 'job_id', 'fk_benefit_job', 'silver.silver_job_postings', 'job_id'),
                    ('silver.silver_bridge_job_benefits', 'benefit_id', 'fk_benefit_dim', 'silver.silver_benefits', 'benefit_id'),
                    ('silver.silver_bridge_job_industries', 'job_id', 'fk_industry_job', 'silver.silver_job_postings', 'job_id'),
                    ('silver.silver_bridge_job_industries', 'industry_id', 'fk_industry_dim', 'silver.silver_industries', 'industry_id'),
                    ('silver.silver_bridge_job_cities', 'job_id', 'fk_city_job', 'silver.silver_job_postings', 'job_id'),
                    ('silver.silver_bridge_job_cities', 'city_id', 'fk_city_dim', 'silver.silver_cities', 'city_id'),
                    ('silver.silver_bridge_job_jobfunctions', 'job_id', 'fk_function_job', 'silver.silver_job_postings', 'job_id'),
                    ('silver.silver_bridge_job_jobfunctions', 'job_function_id', 'fk_function_dim', 'silver.silver_job_functions', 'job_function_id'),
                    ('silver.silver_bridge_job_group_jobfunctions', 'job_id', 'fk_groupfunc_job', 'silver.silver_job_postings', 'job_id'),
                    ('silver.silver_bridge_job_group_jobfunctions', 'group_job_function_id', 'fk_groupfunc_dim', 'silver.silver_group_job_functions', 'group_job_function_id')
                ]
                
                for s_table, s_col, fk_name, t_table, t_col in fk_config:
                    print(f"Tạo FK: {s_table}.{s_col} -> {t_table}.{t_col}")
                    try:
                        conn.execute(f"""
                            ALTER TABLE {s_table}
                            ADD CONSTRAINT {fk_name}
                                FOREIGN KEY({s_col}) 
                                REFERENCES {t_table}({t_col})
                                ON DELETE SET NULL;
                        """)
                    except Exception as e:
                        if "does not exist" in str(e):
                            print(f"CẢNH BÁO: Bảng {s_table} hoặc {t_table} không tồn tại (do web sập). Bỏ qua tạo FK.")
                        else:
                            raise e

            print("Hoàn thành Transform Silver Layer (đã commit).")
                
    except Exception as e:
        print(f"LỖI trong quá trình Transform Silver: {e}")
        raise
