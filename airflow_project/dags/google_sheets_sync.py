from airflow import DAG
from airflow.operators.python import PythonOperator # type:ignore
from datetime import datetime, timedelta

from logic.google_sheets_sync.google_sheets_pusher import push_mart_to_google_sheets

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
    schedule_interval='0 4 * * *', # Chạy lúc 4:00 sáng (1 tiếng sau DAG 1)
    catchup=False
) as dag:
    
    # URL TỔNG (bạn đã cung cấp)
    MASTER_SHEET_URL = "https://docs.google.com/spreadsheets/d/1nyuCMHxXY1f2-vEaKGhgVqrW9wQzOiUZVvluW_Jl2rA/edit?gid=0#gid=0" 

    task_push_kpis = PythonOperator(
        task_id='push_mart_kpis_over_time',
        python_callable=push_mart_to_google_sheets, 
        op_kwargs={
            'mart_table_name': 'mart_kpis_over_time',
            'sheet_url': MASTER_SHEET_URL,
            'target_tab_name': 'KPIs Over Time'
        }
    )
    
    task_push_skills = PythonOperator(
        task_id='push_mart_skills_deep_dive',
        python_callable=push_mart_to_google_sheets,
        op_kwargs={
            'mart_table_name': 'mart_skills_deep_dive', 
            'sheet_url': MASTER_SHEET_URL,
            'target_tab_name': 'Skills Analysis'
        }
    )
    
    task_push_locations = PythonOperator(
        task_id='push_mart_locations_analysis',
        python_callable=push_mart_to_google_sheets,
        op_kwargs={
            'mart_table_name': 'mart_locations_analysis',
            'sheet_url': MASTER_SHEET_URL,
            'target_tab_name': 'Locations Analysis'
        }
    )

    task_push_industries = PythonOperator(
        task_id='push_mart_industries_analysis',
        python_callable=push_mart_to_google_sheets,
        op_kwargs={
            'mart_table_name': 'mart_industries_analysis', 
            'sheet_url': MASTER_SHEET_URL,
            'target_tab_name': 'Industries Analysis' # Tab 4
        }
    )

    task_push_job_functions = PythonOperator(
        task_id='push_mart_job_functions_analysis',
        python_callable=push_mart_to_google_sheets,
        op_kwargs={
            'mart_table_name': 'mart_job_functions_analysis', 
            'sheet_url': MASTER_SHEET_URL,
            'target_tab_name': 'Job Functions Analysis' # Tab 5
        }
    )

    task_push_benefits = PythonOperator(
        task_id='push_mart_benefits_analysis',
        python_callable=push_mart_to_google_sheets,
        op_kwargs={
            'mart_table_name': 'mart_benefits_analysis', 
            'sheet_url': MASTER_SHEET_URL,
            'target_tab_name': 'Benefits Analysis' # Tab 6
        }
    )