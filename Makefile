# ==========================================
# Makefile — PUBG Anti-Cheat Data Platform
# Multi-language Monorepo Management (Go, Rust, R)
# ==========================================

# Khai báo các phony target không liên quan tới file vật lý
.PHONY: help init fmt test clean lint check-deps up down logs

# Target mặc định khi chỉ gõ `make`
.DEFAULT_GOAL := help

## help: Hiển thị danh sách các lệnh Make khả dụng kèm mô tả
help:
	@echo "======================================================="
	@echo " PUBG Anti-Cheat Data Platform — Monorepo Commands"
	@echo "======================================================="
	@sed -n 's/^##//p' $(MAKEFILE_LIST) | column -t -s ':' | sed -e 's/^/ /'

## check-deps: Kiểm tra các công cụ hệ thống bắt buộc (Go, Cargo, R, Docker)
check-deps:
	@echo "[+] Kiểm tra các công cụ lập trình cần thiết..."
	@command -v go >/dev/null 2>&1 && echo "  - Go: OK" || echo "  - Go: Chưa cài đặt"
	@command -v cargo >/dev/null 2>&1 && echo "  - Rust (Cargo): OK" || echo "  - Rust (Cargo): Chưa cài đặt"
	@command -v Rscript >/dev/null 2>&1 && echo "  - R: OK" || echo "  - R: Chưa cài đặt"
	@command -v docker >/dev/null 2>&1 && echo "  - Docker: OK" || echo "  - Docker: Chưa cài đặt"

## init: Khởi tạo file môi trường .env từ .env.example
init:
	@echo "[+] Khởi tạo file cấu hình môi trường .env..."
	@if [ ! -f .env ]; then \
		cp .env.example .env; \
		echo "  - Đã tạo thành công file .env từ .env.example"; \
	else \
		echo "  - File .env đã tồn tại, bỏ qua."; \
	fi

## fmt: Định dạng mã nguồn cho tất cả các service (Go, Rust, R)
fmt:
	@echo "[+] Đang định dạng mã nguồn Go..."
	@if [ -d apps/go-ingestor ] && [ -f apps/go-ingestor/go.mod ]; then \
		cd apps/go-ingestor && go fmt ./...; \
	fi
	@echo "[+] Đang định dạng mã nguồn Rust..."
	@if [ -d apps/rust-processor ] && [ -f apps/rust-processor/Cargo.toml ]; then \
		cd apps/rust-processor && cargo fmt; \
	fi

## test: Chạy unit test toàn bộ các service trong Monorepo
test:
	@echo "[+] Đang chạy unit test Go Ingestor..."
	@if [ -d apps/go-ingestor ] && [ -f apps/go-ingestor/go.mod ]; then \
		cd apps/go-ingestor && go test -v ./...; \
	fi
	@echo "[+] Đang chạy unit test Rust Processor..."
	@if [ -d apps/rust-processor ] && [ -f apps/rust-processor/Cargo.toml ]; then \
		cd apps/rust-processor && cargo test; \
	fi

## clean: Dọn dẹp các file build tạm, log và dữ liệu rác
clean:
	@echo "[+] Dọn dẹp dữ liệu tạm và file build..."
	@rm -rf bin/ target/ tmp/ *.log
	@find . -name "*.tmp" -type f -delete
	@echo "  - Đã hoàn tất dọn dẹp."

## up: Khởi chạy hạ tầng Kafka & MinIO qua Docker Compose
up:
	@echo "[+] Khởi chạy hạ tầng Kafka & MinIO Container..."
	docker compose up -d

## down: Dừng và giải phóng toàn bộ hạ tầng Docker Compose
down:
	@echo "[+] Dừng và giải phóng các container hạ tầng..."
	docker compose down

## logs: Theo dõi log thời gian thực từ các container hạ tầng
logs:
	docker compose logs -f

