# Makefile for the CareerBI Airflow Project

.PHONY: all up down start stop restart ui db-dwh db-meta logs help

all: up

up:
	@echo "🚀 Starting Airflow (Webserver, Scheduler) and SQL Server..."
	@echo "Lần đầu chạy sẽ mất thời gian để build image (cài driver MSSQL)..."
	docker-compose up -d --build

down:
	@echo "🛑 Stopping and removing all services..."
	docker-compose down

start: up
stop: down
restart: down up

ui:
	@echo "Opening Airflow UI at http://localhost:8080..."
	@if [ "$(shell uname)" = "Darwin" ]; then \
		open http://localhost:8080; \
	else \
		xdg-open http://localhost:8080; \
	fi

# Truy cập DB DWH (careerbi)
db-dwh:
	@echo "Connecting to SQL Server DWH (database: careerbi)..."
	@echo "Gõ 'exit' để thoát."
	docker-compose exec sqlserver /opt/mssql-tools/bin/sqlcmd -S localhost -U sa -d careerbi -P ${MSSQL_SA_PASSWORD}

# Truy cập DB Metadata (airflow_meta)
db-meta:
	@echo "Connecting to SQL Server Metadata (database: airflow_meta)..."
	@echo "Gõ 'exit' để thoát."
	docker-compose exec sqlserver /opt/mssql-tools/bin/sqlcmd -S localhost -U sa -d ${AIRFLOW_METADATA_DB} -P ${MSSQL_SA_PASSWORD}

logs:
	@echo "Tailing logs for all services (Press Ctrl+C to exit)..."
	docker-compose logs -f --tail=100

help:
	@echo "Available commands for CareerBI project:"
	@echo "------------------------------------------------"
	@echo "  make up        : Khởi động tất cả dịch vụ (build image nếu cần)."
	@echo "  make down      : Dừng và gỡ bỏ tất cả dịch vụ."
	@echo "  make restart   : Khởi động lại tất cả dịch vụ."
	@echo "  make ui        : Mở giao diện Web Airflow (localhost:8080)."
	@echo "  make db-dwh    : Truy cập vào DB Data Warehouse (careerbi)."
	@echo "  make db-meta   : Truy cập vào DB Metadata của Airflow (airflow_meta)."
	@echo "  make logs      : Xem log của các dịch vụ đang chạy."