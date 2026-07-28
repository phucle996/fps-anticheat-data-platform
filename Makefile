# ==========================================
# Makefile — PUBG Anti-Cheat Data Platform
# Multi-language Monorepo Management (Go, Rust, R, Python)
# ==========================================

# Khai báo các phony target không liên quan tới file vật lý
.PHONY: help init start run stop restart purge fmt test clean logs check-deps up down

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
	@echo "[+] 5. Khởi tạo các Kafka Topics trên container pubg-kafka..."
	@docker exec pubg-kafka kafka-topics --bootstrap-server localhost:9092 --create --if-not-exists --topic pubg.v1.player-stat.raw --partitions 1 --replication-factor 1 > /dev/null 2>&1 || true
	@docker exec pubg-kafka kafka-topics --bootstrap-server localhost:9092 --create --if-not-exists --topic pubg.v1.invalid --partitions 1 --replication-factor 1 > /dev/null 2>&1 || true
	@docker exec pubg-kafka kafka-topics --bootstrap-server localhost:9092 --create --if-not-exists --topic pubg.v1.dataset.gold.ready --partitions 1 --replication-factor 1 > /dev/null 2>&1 || true
	@docker exec pubg-kafka kafka-topics --bootstrap-server localhost:9092 --create --if-not-exists --topic pubg.v1.ml.model.ready --partitions 1 --replication-factor 1 > /dev/null 2>&1 || true
	@echo "[+] 6. Tải và đồng bộ Dataset thực tế lên MinIO S3 (Hiển thị Progress Bar %)..."
	docker run --rm --network pubg-platform-net \
		-v $(PWD)/apps/go-ingestor:/app -w /app \
		-e KAFKA_BROKERS="kafka:9092" \
		-e KAFKA_RAW_TOPIC="pubg.v1.player-stat.raw" \
		-e KAFKA_INVALID_TOPIC="pubg.v1.invalid" \
		-e MINIO_ENDPOINT="minio:9000" \
		-e MINIO_ACCESS_KEY="minioadmin" \
		-e MINIO_SECRET_KEY="minioadmin" \
		-e MINIO_BUCKET="fps-anticheat-datalake" \
		-e KAGGLE_DATASET_SLUG="skihikingkevin/pubg-match-deaths" \
		-e KAGGLE_SELECTED_FILE="deaths.csv" \
		golang:1.26-alpine go run ./cmd/dataset-sync
	@echo "  ✓ Khởi tạo toàn bộ môi trường hoàn tất 100%!"

## start: Khởi chạy toàn bộ hạ tầng Docker Compose stack
start:
	@echo "[+] Khởi chạy toàn bộ dịch vụ Docker Containers..."
	docker compose -f deployments/compose/docker-compose.yml up -d

## up: Alias của make start
up: start

## run: Thực thi Replay ingest liên tục (Resume từ Checkpoint nếu đã ngắt trước đó)
run:
	@echo "[+] 1. Kích hoạt Go Ingestor Replay Engine (Checkpoint Resume Mode)..."
	@echo "    ℹ️  Nếu đã chạy trước đó, tiến trình sẽ tự động Resume từ vị trí checkpoint cuối cùng."
	@echo "    ℹ️  Nếu muốn chạy lại từ đầu, hãy dùng: make run-reset"
	docker run --rm --network pubg-platform-net \
		-v $(PWD)/apps/go-ingestor:/app -w /app \
		-e KAFKA_BROKERS="kafka:9092" \
		-e MINIO_ENDPOINT="minio:9000" \
		-e MINIO_ACCESS_KEY="minioadmin" \
		-e MINIO_SECRET_KEY="minioadmin" \
		-e MINIO_BUCKET="fps-anticheat-datalake" \
		-e KAGGLE_DATASET_SLUG="skihikingkevin/pubg-match-deaths" \
		-e KAGGLE_SELECTED_FILE="deaths.csv" \
		-e KAFKA_RAW_TOPIC="pubg.v1.player-stat.raw" \
		-e KAFKA_INVALID_TOPIC="pubg.v1.invalid" \
		golang:1.26-alpine go run ./cmd/replay -limit=0 -stream-delay-ms=0 -dry-run=false -disable-checkpoint=false
	@echo "[+] 2. Kích hoạt R Processor Medallion ETL (Silver & Gold Feature Engine)..."
	@cd apps/rust-processor/r-processor && Rscript tests/test_silver_preprocessor.R > /dev/null 2>&1 && Rscript tests/test_gold_feature_engine.R > /dev/null 2>&1 && Rscript tests/test_eda_analyzer.R > /dev/null 2>&1
	@echo "  ✓ Phát dữ liệu thực tế và flush xuống S3 Data Lake hoàn tất 100%!"

## run-reset: Xóa Checkpoint cũ và phát lại toàn bộ dữ liệu từ đầu (dòng 1)
run-reset:
	@echo "[+] 🔄 Xóa Checkpoint cũ, phát lại toàn bộ CSV từ dòng 1..."
	docker run --rm --network pubg-platform-net \
		-v $(PWD)/apps/go-ingestor:/app -w /app \
		-e KAFKA_BROKERS="kafka:9092" \
		-e MINIO_ENDPOINT="minio:9000" \
		-e MINIO_ACCESS_KEY="minioadmin" \
		-e MINIO_SECRET_KEY="minioadmin" \
		-e MINIO_BUCKET="fps-anticheat-datalake" \
		-e KAGGLE_DATASET_SLUG="skihikingkevin/pubg-match-deaths" \
		-e KAGGLE_SELECTED_FILE="deaths.csv" \
		-e KAFKA_RAW_TOPIC="pubg.v1.player-stat.raw" \
		-e KAFKA_INVALID_TOPIC="pubg.v1.invalid" \
		golang:1.26-alpine go run ./cmd/replay -limit=0 -stream-delay-ms=0 -dry-run=false -disable-checkpoint=false -reset-checkpoint=true
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
