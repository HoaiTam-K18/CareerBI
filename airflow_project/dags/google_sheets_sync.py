# dags/google_sheets_sync.py
from airflow import DAG
from airflow.sensors.external_task import ExternalTaskSensor # type:ignore
from airflow.operators.python import PythonOperator # type:ignore
from datetime import datetime, timedelta

# === IMPORT MỚI ===
# Import hàm "logic" của bạn từ file kia
from logic.google_sheets_sync.google_sheets_pusher import push_mart_to_google_sheets


# (XÓA hết các import gspread, pandas, sqlalchemy... ở đây)

# === CẤU HÌNH DAG 2 ===
default_args = {
    'owner': 'careerbi_admin',
    'depends_on_past': False,
    'start_date': datetime(2025, 11, 13),
    'retries': 1,
    'retry_delay': timedelta(minutes=2),
}

with DAG(
    'google_sheets_sync',
    default_args=default_args,
    schedule_interval='0 4 * * *',
    catchup=False
) as dag:
    
    # === TASK 1, 2, 3: Đẩy 3 Marts vào 1 Sheet "TỔNG" ===
    
    MASTER_SHEET_URL = "https://docs.google.com/spreadsheets/d/1nyuCMHxXY1f2-vEaKGhgVqrW9wQzOiUZVvluW_Jl2rA/edit?gid=0#gid=0" 

    task_push_jobs = PythonOperator(
        task_id='push_mart_jobs_over_time',
        # Chỉ cần "gọi" hàm đã import
        python_callable=push_mart_to_google_sheets, 
        op_kwargs={
            'mart_table_name': 'mart_jobs_over_time', 
            'sheet_url': MASTER_SHEET_URL,
            'target_tab_name': 'KPIs Over Time'
        }
    )
    
    task_push_skills = PythonOperator(
        task_id='push_mart_skills_over_time',
        python_callable=push_mart_to_google_sheets,
        op_kwargs={
            'mart_table_name': 'mart_skills_over_time', 
            'sheet_url': MASTER_SHEET_URL,
            'target_tab_name': 'Skills Analysis'
        }
    )
    
    task_push_cities = PythonOperator(
        task_id='push_mart_cities_over_time',
        python_callable=push_mart_to_google_sheets,
        op_kwargs={
            'mart_table_name': 'mart_cities_over_time', 
            'sheet_url': MASTER_SHEET_URL,
            'target_tab_name': 'Cities Analysis'
        }
    )
