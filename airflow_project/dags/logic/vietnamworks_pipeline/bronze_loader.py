# dags/logic/bronze_loader.py
# LOGIC TASK 2: BRONZE

import os
import pandas as pd
from datetime import datetime
from .db_utils import get_db_engine # Import từ file chung

def get_daily_folder_path():
    """Hàm này lấy đường dẫn thư mục theo ngày (tuyệt đối)."""
    crawl_date_str = datetime.now().strftime('%d-%m-%Y')
    return f"/opt/airflow/data/clean/{crawl_date_str}"

def load_bronze():
    """
    Tải CSV vào các bảng trong schema 'bronze' VỚI CHIẾN LƯỢC APPEND.
    """
    print("Bắt đầu tác vụ Tải (Load) vào Bronze Layer...")
    data_dir = get_daily_folder_path()
    bronze_schema = "bronze"
    current_load_date = datetime.now().date()

    files_to_load = {
        "job_postings.csv": "bronze_job_postings",
        "job_timelife.csv": "bronze_job_timelife",
        "companies.csv": "bronze_companies",
        "cities.csv": "bronze_cities",
        "skills.csv": "bronze_skills",
        "benefits.csv": "bronze_benefits",
        "industries.csv": "bronze_industries",
        "job_functions.csv": "bronze_job_functions",
        "group_job_functions.csv": "bronze_group_job_functions",
        "job_skills.csv": "bronze_bridge_job_skills",
        "job_benefits.csv": "bronze_bridge_job_benefits",
        "job_industries.csv": "bronze_bridge_job_industries",
        "job_cities.csv": "bronze_bridge_job_cities",
        "job_jobFunctions.csv": "bronze_bridge_job_jobFunctions",
        "job_group_jobFunctions.csv": "bronze_bridge_job_group_jobFunctions"
    }

    try:
        with get_db_engine() as engine:
            with engine.connect() as conn:
                for csv_file, table_name in files_to_load.items():
                    file_path = os.path.join(data_dir, csv_file)
                    if os.path.exists(file_path):
                        df = pd.read_csv(file_path, dtype=str)
                        df['load_date'] = current_load_date
                        df.to_sql(table_name, conn, if_exists='append', index=False, schema=bronze_schema)
                    else:
                        print(f"LỖI NGHIÊM TRỌNG: Không tìm thấy file {file_path}. Dừng task.")
                        raise FileNotFoundError(f"File bắt buộc {file_path} không tìm thấy.")
        print("Hoàn thành Tải Bronze.")
    except Exception as e:
        print(f"LỖI trong quá trình Load Bronze: {e}")
        raise