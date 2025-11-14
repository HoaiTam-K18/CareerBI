from airflow import DAG
from airflow.operators.bash import BashOperator # type:ignore
from airflow.operators.python import PythonOperator # type:ignore
from datetime import datetime, timedelta

# <<< IMPORT 2 HÀM LOGIC TỪ 2 FILE RIÊNG BIỆT >>>
from logic.vietnamworks_pipeline.bronze_loader import load_bronze
from logic.vietnamworks_pipeline.silver_transformer import transform_silver

DBT_PROJECT_DIR = "/opt/airflow/dbt_project"

# ===================================================
# ĐỊNH NGHĨA DAG
# ===================================================

default_args = {
    'owner': 'careerbi_admin',
    'depends_on_past': False,
    'start_date': datetime(2025, 11, 6),
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

with DAG(
    'vietnamworks_pipeline',
    default_args=default_args,
    description='(ELT) E -> L (Bronze) -> T (Silver) -> T (Gold)',
    schedule_interval='0 3 * * *',
    catchup=False,
    tags=['elt', 'scrapy', 'medallion', 'refactored'],
) as dag:

    # # TASK 1: Extract (Giữ nguyên)
    # task_extract_scrapy = BashOperator(
    #     task_id='run_scrapy_spider',
    #     bash_command='cd /opt/airflow/scrape_job && scrapy crawl vietnamworks'
    # )

    # TASK 2: Load Bronze
    task_load_bronze = PythonOperator(
        task_id='load_bronze',
        python_callable=load_bronze
    )

    # TASK 3: Transform Silver
    task_transform_silver = PythonOperator(
        task_id='transform_silver',
        python_callable=transform_silver
    )

    # <<< TASK 4: dbt (Tầng Gold) >>>
    task_dbt_run = BashOperator(
        task_id='run_dbt_gold',
        
        # SỬA LẠI: Chỉ định rõ ràng mọi đường dẫn, không cần 'cd'
        bash_command=(
            f"dbt build "
            f"--project-dir {DBT_PROJECT_DIR} "  # Nơi chứa dbt_project.yml
            f"--profiles-dir {DBT_PROJECT_DIR} " # Nơi chứa profiles.yml
            f"--target prod"                     # Dùng cấu hình 'prod'
        )
    )

    # THỨ TỰ (Thêm Task 4) 
    # task_extract_scrapy >> 
    task_load_bronze >> task_transform_silver >> task_dbt_run