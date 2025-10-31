# Makefile for the CareerBI Airflow Project

# Định nghĩa các target này là "ảo" (không phải file)
# Điều này đảm bảo 'make' luôn chạy lệnh ngay cả khi có file trùng tên
.PHONY: all up down start stop restart ui db logs help

# Target mặc định, chạy khi bạn chỉ gõ "make"
all: up

# Khởi động tất cả dịch vụ (ở chế độ nền)
up:
	@echo "🚀 Starting Airflow (Webserver, Scheduler) and Postgres..."
	docker-compose up -d

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

# Target trợ giúp, liệt kê các lệnh
help:
	@echo "Available commands for CareerBI project:"
	@echo "------------------------------------------------"
	@echo "  make up        : Khởi động tất cả dịch vụ."
	@echo "  make down      : Dừng và gỡ bỏ tất cả dịch vụ."
	@echo "  make restart   : Khởi động lại tất cả dịch vụ."
	@echo "  make ui        : Mở giao diện Web Airflow (localhost:8080)."
	@echo "  make db        : Truy cập vào shell database 'careerbi'."
	@echo "  make logs      : Xem log của các dịch vụ đang chạy."