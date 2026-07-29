# ==========================================
# Makefile — PUBG Anti-Cheat Data Platform
# Multi-language Monorepo Management (Go, Rust, R, Python)
# ==========================================

# Khai báo các phony target không liên quan tới file vật lý
.PHONY: help init start run run-reset stop restart purge fmt test clean logs check-deps up down build-ingestor

# Target mặc định khi gõ `make`
.DEFAULT_GOAL := help

## help: Hiển thị danh sách các lệnh Make khả dụng kèm mô tả chi tiết
help:
	@echo "====================================================================="
	@echo " PUBG Anti-Cheat Data Platform — Monorepo Commands"
	@echo "====================================================================="
	@sed -n 's/^##//p' $(MAKEFILE_LIST) | column -t -s ':' | sed -e 's/^/ /'

## check-deps: Kiểm tra các công cụ hệ thống bắt buộc (Go, Cargo, R, Python, Docker)
check-deps:
	@echo "[+] Kiểm tra các công cụ lập trình cần thiết..."
	@command -v go >/dev/null 2>&1 && echo "  - Go: OK" || echo "  - Go: Chưa cài đặt"
	@command -v cargo >/dev/null 2>&1 && echo "  - Rust (Cargo): OK" || echo "  - Rust (Cargo): Chưa cài đặt"
	@command -v Rscript >/dev/null 2>&1 && echo "  - R: OK" || echo "  - R: Chưa cài đặt"
	@command -v python3 >/dev/null 2>&1 && echo "  - Python3: OK" || echo "  - Python3: Chưa cài đặt"
	@command -v docker >/dev/null 2>&1 && echo "  - Docker: OK" || echo "  - Docker: Chưa cài đặt"

## init: Khởi tạo file .env, bật toàn bộ stack Docker Containers và khởi tạo S3 Buckets / Kafka Topics / Medallion Data Lake
init:
	# Dọn dẹp các container mồ côi cũ (name=pubg-* hoặc command=/app/rust-processor) trước khi tạo mới để tránh lỗi conflict container name
	@echo "[+] 0. Dọn dẹp các container mồ côi cũ trước khi khởi tạo..."
	@docker ps -a --format '{{.ID}} {{.Command}} {{.Names}}' | grep -E '/app/rust-processor|pubg-' | awk '{print $$1}' | xargs -r docker rm -f > /dev/null 2>&1 || true
	@echo "[+] 1. Khởi tạo file môi trường .env từ .env.example..."
	@if [ ! -f .env ]; then cp .env.example .env; fi
	@echo "[+] 2. Khởi chạy toàn bộ stack Docker Containers (Kafka, MinIO, Kafka UI, Rust Processor, ML Platform, Streamlit)..."
	docker compose -f deployments/compose/docker-compose.yml up -d --build
	@echo "[+] 3. Đợi container hạ tầng và ứng dụng sẵn sàng..."
	@sleep 5
	@echo "[+] 4. Khởi tạo 3 Buckets S3 & 9 tầng thư mục Medallion Data Lake trên MinIO..."
	@docker run --rm --network pubg-platform-net -e MINIO_ENDPOINT="http://minio:9000" -v $(PWD)/scripts/init_minio_datalake.sh:/init.sh --entrypoint /bin/sh minio/mc -c "mc alias set localminio http://minio:9000 minioadmin minioadmin && /init.sh" > /dev/null 2>&1 || true
	@docker run --rm --network pubg-platform-net minio/mc mb --ignore-existing localminio/pubg-models > /dev/null 2>&1 || true
	@docker run --rm --network pubg-platform-net minio/mc mb --ignore-existing localminio/pubg-predictions > /dev/null 2>&1 || true
	@echo "[+] 5. Kafka Topics được tạo tự động bởi init-kafka container (scripts/create-topics.sh — source of truth)."
	@echo "       Topics: pubg.v1.player-stat.raw (6p), pubg.v1.kill-event.raw (6p), pubg.v1.invalid (3p),"
	@echo "               pubg.v1.dataset.gold.ready (1p), pubg.v1.ml.model.ready (1p)"
	@echo "[+] 6. Build image Go Ingestor (nếu chưa có) và tải Dataset lên MinIO S3..."
	# Build image chứa cả 2 binary (dataset-sync + replay) — cache BuildKit nên lần sau rất nhanh
	docker compose -f deployments/compose/docker-compose.yml build go-ingestor-sync
	# Dùng image đã build — không cần golang:1.26-alpine go run, không download dependency
	docker compose -f deployments/compose/docker-compose.yml run --rm go-ingestor-sync
	@echo "  ✓ Khởi tạo toàn bộ môi trường hoàn tất 100%!"

## start: Khởi chạy toàn bộ hạ tầng Docker Compose stack
start:
	@echo "[+] Khởi chạy toàn bộ dịch vụ Docker Containers..."
	docker compose -f deployments/compose/docker-compose.yml up -d

## up: Alias của make start
up: start



## build-ingestor: Build Docker image go-ingestor (chứa cả dataset-sync & replay binary)
build-ingestor:
	@echo "[+] Build Docker image go-ingestor (2 binary: dataset-sync + replay)..."
	docker compose -f deployments/compose/docker-compose.yml build go-ingestor-sync
	@echo "  ✓ Image go-ingestor đã sẵn sàng!"

## run: Stream Replay liên tục (Auto-Resume từ Checkpoint MinIO S3 nếu đã ngắt)
run:
	@echo "[+] 1. Kích hoạt Go Ingestor Replay Engine (Checkpoint Resume Mode)..."
	@echo "    ℹ️  Nếu đã chạy trước đó, tiến trình sẽ tự động Resume từ vị trí checkpoint cuối cùng."
	@echo "    ℹ️  Nếu muốn chạy lại từ đầu, hãy dùng: make run-reset"
	# Build image nếu chưa có hoặc code thay đổi — BuildKit cache nhanh khi không đổi dependency
	docker compose -f deployments/compose/docker-compose.yml build go-ingestor-replay
	# Dùng image đã build sẵn — không cần golang:1.26-alpine go run, không download dependency
	docker compose -f deployments/compose/docker-compose.yml run --rm go-ingestor-replay

## run-reset: Xóa Checkpoint cũ và phát lại toàn bộ dữ liệu từ đầu (dòng 1)
run-reset:
	@echo "[+] 🔄 Xóa Checkpoint cũ, phát lại toàn bộ CSV từ dòng 1..."
	# Override command của service go-ingestor-replay thêm cờ -reset-checkpoint=true
	docker compose -f deployments/compose/docker-compose.yml run --rm go-ingestor-replay \
		/app/bin/replay -limit=0 -stream-delay-ms=0 -dry-run=false -disable-checkpoint=false -reset-checkpoint=true
	@echo "  ✓ Reset hoàn tất, phát lại dữ liệu từ đầu thành công!"

## stop: Tạm dừng toàn bộ các containers đang chạy (bảo toàn volume dữ liệu)
stop:
	@echo "[+] Tạm dừng toàn bộ các containers (giữ nguyên data volume)..."
	docker compose -f deployments/compose/docker-compose.yml stop

## down: Dừng và giải phóng container hạ tầng
down: stop

## restart: Tái khởi động lại toàn bộ containers
restart:
	@echo "[+] Tái khởi động lại toàn bộ stack containers..."
	docker compose -f deployments/compose/docker-compose.yml restart

## purge: Dừng container, xóa toàn bộ volume Docker và làm sạch dữ liệu rác (Zero-State Clean)
purge:
	@echo "[+] Dừng và dọn dẹp triệt để Docker Containers & Data Volumes..."
	docker compose -f deployments/compose/docker-compose.yml down -v --remove-orphans
	# Xóa sạch tất cả các container mồ côi (standalone containers) khởi tạo trực tiếp qua `docker run` hoặc test suite
	@docker ps -a --format '{{.ID}} {{.Command}} {{.Names}}' | grep -E '/app/rust-processor|pubg-' | awk '{print $$1}' | xargs -r docker rm -f > /dev/null 2>&1 || true
	@docker network rm pubg-platform-net > /dev/null 2>&1 || true
	@echo "[+] Xóa sạch cache, log và file tạm local..."
	@rm -rf bin/ target/ tmp/ *.log apps/rust-processor/target apps/ml-platform/rust-inference/target
	@find . -name "*.tmp" -type f -delete
	@echo "  ✓ Đã xóa sạch 100% dữ liệu, hệ thống đưa về trạng thái sạch sẽ ban đầu!"

## fmt: Định dạng mã nguồn cho tất cả các service (Go, Rust, Python, R)
fmt:
	@echo "[+] Đang định dạng mã nguồn Go..."
	@if [ -d apps/go-ingestor ] && [ -f apps/go-ingestor/go.mod ]; then cd apps/go-ingestor && go fmt ./...; fi
	@if [ -d apps/ml-platform/go-api ] && [ -f apps/ml-platform/go-api/go.mod ]; then cd apps/ml-platform/go-api && go fmt ./...; fi
	@echo "[+] Đang định dạng mã nguồn Rust..."
	@if [ -d apps/rust-processor ] && [ -f apps/rust-processor/Cargo.toml ]; then cd apps/rust-processor && cargo fmt; fi
	@if [ -d apps/ml-platform/rust-inference ] && [ -f apps/ml-platform/rust-inference/Cargo.toml ]; then cd apps/ml-platform/rust-inference && cargo fmt; fi

## test: Chạy unit test toàn bộ các service trong Monorepo
test:
	@echo "[+] Unit test Go Ingestor..."
	@cd apps/go-ingestor && go test ./...
	@echo "[+] Unit test Go API Gateway..."
	@cd apps/ml-platform/go-api && go test ./...
	@echo "[+] Unit test Rust Processor..."
	@cd apps/rust-processor && cargo test
	@echo "[+] Unit test Rust Inference Engine..."
	@cd apps/ml-platform/rust-inference && cargo test
	@echo "[+] Unit test Python ML Worker..."
	@cd apps/ml-platform/python-ml-worker && venv/bin/python -m pytest
	@echo "[+] Unit test Streamlit UI Dashboard..."
	@cd apps/streamlit-dashboard && venv/bin/python -m pytest

## clean: Dọn dẹp các file build tạm và log
clean:
	@echo "[+] Dọn dẹp dữ liệu tạm và file build..."
	@rm -rf bin/ target/ tmp/ *.log
	@find . -name "*.tmp" -type f -delete
	@echo "  - Đã hoàn tất dọn dẹp."

## logs: Theo dõi log thời gian thực từ các container hạ tầng
logs:
	docker compose -f deployments/compose/docker-compose.yml logs -f
