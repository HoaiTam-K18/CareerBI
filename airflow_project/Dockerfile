# (Đặt file này trong /airflow_project/Dockerfile)

# Bắt đầu từ image Airflow gốc
FROM apache/airflow:2.8.1

# Cài đặt TẤT CẢ các thư viện Python mà các Task của bạn cần:
# 1. Scrapy (cho Task 1 - BashOperator)
# 2. pandas (cho Task 2 - PythonOperator)
# 3. SQLAlchemy (cho Task 2 - PythonOperator)
# (psycopg2 đã có sẵn trong image)
RUN pip install scrapy pandas SQLAlchemy