# File: Dockerfile
# (Đặt file này cùng cấp với docker-compose.yaml)

# Bắt đầu từ image Airflow gốc
FROM apache/airflow:2.8.1

# Chuyển sang user 'root' để cài đặt phần mềm
USER root

# Cài đặt các thư viện hệ thống cần thiết cho ODBC Driver của Microsoft
RUN apt-get update && \
    apt-get install -y curl gnupg && \
    curl https://packages.microsoft.com/keys/microsoft.asc | apt-key add - && \
    curl https://packages.microsoft.com/config/debian/11/prod.list > /etc/apt/sources.list.d/mssql-release.list && \
    apt-get update && \
    # Cài đặt ODBC Driver
    ACCEPT_EULA=Y apt-get install -y msodbcsql17 && \
    # Cài đặt command line tools (để debug)
    ACCEPT_EULA=Y apt-get install -y mssql-tools && \
    apt-get clean && \
    rm -rf /var/lib/apt/lists/*

# Trở lại user 'airflow'
USER airflow

# Cài đặt thư viện Python cho Airflow (bao gồm cả 'pyodbc')
RUN pip install apache-airflow-providers-microsoft-mssql