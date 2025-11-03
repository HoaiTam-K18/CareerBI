# Makefile for the CareerBI Airflow Project

# Target mặc định, chạy khi bạn chỉ gõ "make"
.PHONY: all up down start stop restart ui db logs help clean-logs full-reset
all: up

# Khởi động tất cả dịch vụ (ở chế độ nền)
# (Thêm --build để nó xây dựng Dockerfile, cài scrapy/pandas)
up:
	@echo "🚀 Starting Airflow (Webserver, Scheduler) and Postgres..."
	@echo "Building custom image with scrapy/pandas/sqlalchemy..."
	docker-compose up -d --build

# Dừng và gỡ bỏ tất cả container
down:
	@echo "🛑 Stopping and removing all Airflow containers..."
	docker-compose down

# 'start' và 'stop' là các tên gọi khác (alias) cho tiện
start: up
stop: down

# Khởi động lại toàn bộ dịch vụ
restart: down up

# Mở giao diện Web Airflow
ui:
	@echo "Opening Airflow UI at http://localhost:8080..."
	@echo "Nếu trình duyệt không tự mở, hãy truy cập: http://localhost:8080"
	@if [ "$(shell uname)" = "Darwin" ]; then \
		open http://localhost:8080; \
	else \
		xdg-open http://localhost:8080; \
	fi

# Truy cập vào shell psql của database 'careerbi'
db:
	@echo "Connecting to PostgreSQL database 'careerbi' as user 'careerbi_user'..."
	@echo "Gõ '\\q' để thoát."
	docker-compose exec postgres psql -U careerbi_user -d careerbi

# Xem log của tất cả các service
logs:
	@echo "Tailing logs for all services (Press Ctrl+C to exit)..."
	docker-compose logs -f --tail=100

# ===================================================================================
# LỆNH DỌN DẸP (NÂNG CAO)
# ===================================================================================

# Lệnh này chỉ xóa file log (an toàn)
clean-logs:
	@echo "🧹 Cleaning up log files..."
	@echo "Lưu ý: Có thể bạn cần nhập mật khẩu (sudo) để xóa file do Docker tạo"
	sudo rm -rf ./logs/*
	@echo "Done."

# Lệnh này reset TOÀN BỘ (NGUY HIỂM)
# Nó sẽ xóa hết database, log, và dừng container
full-reset:
	@echo "🔥 WARNING: This will delete ALL containers, logs, AND THE DATABASE. 🔥"
	@read -p "Bạn có chắc chắn muốn reset toàn bộ? (y/N) " -r; \
	if [[ $$REPLY =~ ^[Yy]$$ ]]; then \
		echo "Nuking all data..."; \
		docker-compose down -v; \
		sudo rm -rf ./logs/*; \
		sudo rm -rf ./data/clean/*; \
		echo "Full reset complete. Bạn có thể chạy 'make up' để bắt đầu lại từ đầu."; \
	else \
		echo "Hủy bỏ."; \
	fi

# ===================================================================================

# Target trợ giúp, liệt kê các lệnh
help:
	@echo "Available commands for CareerBI project:"
	@echo "------------------------------------------------"
	@echo "  make up        : Khởi động tất cả dịch vụ (build image nếu cần)."
	@echo "  make down      : Dừng và gỡ bỏ tất cả dịch vụ."
	@echo "  make restart   : Khởi động lại tất cả dịch vụ."
	@echo "  make ui        : Mở giao diện Web Airflow (localhost:8080)."
	@echo "  make db        : Truy cập vào database 'careerbi' (Postgres)."
	@echo "  make logs      : Xem log của các dịch vụ đang chạy."
	@echo "  make clean-logs: (An toàn) Xóa các file log trong thư mục ./logs."
	@echo "  make full-reset: (Nguy hiểm) Xóa hết container, database và logs."