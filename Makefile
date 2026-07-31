# ==========================================
# Makefile — PUBG Anti-Cheat Data Platform
# Multi-language Monorepo Management (Go, Rust, R, Python)
# ==========================================

# Khai báo các phony target không liên quan tới file vật lý
.PHONY: help init start run run-reset stop restart purge fmt test clean logs logs-ml check-deps up down build-ingestor \
        run-kill-0 run-kill-1 run-kill-2 run-kill-3 run-kill-4 \
        run-agg-0 run-agg-1 run-agg-2 run-agg-3 run-agg-4 run-parallel
.PHONY: sync-kill-0 sync-kill-1 sync-kill-2 sync-kill-3 sync-kill-4 bench-kill-parallel

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
	docker compose up -d --build
	@echo "[+] 3. Đợi container hạ tầng và ứng dụng sẵn sàng..."
	@sleep 5
	@echo "[+] 4. Khởi tạo 3 Buckets S3 & 9 tầng thư mục Medallion Data Lake trên MinIO..."
	@docker run --rm --network pubg-platform-net -e MINIO_ENDPOINT="http://minio:9000" -v $(PWD)/scripts/init_minio_datalake.sh:/init.sh --entrypoint /bin/sh minio/mc -c "mc alias set localminio http://minio:9000 minioadmin minioadmin && /init.sh" > /dev/null 2>&1 || true
	@docker run --rm --network pubg-platform-net minio/mc mb --ignore-existing localminio/pubg-models > /dev/null 2>&1 || true
	@docker run --rm --network pubg-platform-net minio/mc mb --ignore-existing localminio/pubg-predictions > /dev/null 2>&1 || true
	@echo "[+] 5. Kafka Topics được tạo tự động bởi init-kafka container (init/create-topics.sh — source of truth)."
	@echo "       Topics: pubg.v1.player-stat.raw (6p), pubg.v1.kill-event.raw (6p), pubg.v1.invalid (3p),"
	@echo "               pubg.v1.dataset.gold.ready (1p), pubg.v1.ml.model.ready (1p)"
	@echo "[+] 6. Build image Go Ingestor (nếu chưa có) và tải Dataset lên MinIO S3..."
	# Build image chứa cả 2 binary (dataset-sync + replay) — cache BuildKit nên lần sau rất nhanh
	docker compose build go-ingestor-sync
	# Dùng image đã build — không cần golang:1.26-alpine go run, không download dependency
	docker compose run --rm go-ingestor-sync
	@echo "  ✓ Khởi tạo toàn bộ môi trường hoàn tất 100%!"

## start: Khởi chạy toàn bộ hạ tầng Docker Compose stack
start:
	@echo "[+] Khởi chạy toàn bộ dịch vụ Docker Containers..."
	docker compose up -d

## up: Alias của make start
up: start

## build-ingestor: Build Docker image go-ingestor (chứa cả dataset-sync & replay binary)
build-ingestor:
	@echo "[+] Build Docker image go-ingestor (2 binary: dataset-sync + replay)..."
	docker compose build go-ingestor-sync go-ingestor-replay
	@echo "  ✓ Image go-ingestor đã sẵn sàng!"

## run: Stream Replay liên tục mặc định (kill_match_stats_final_0.csv)
run:
	@echo "[+] 1. Kích hoạt Go Ingestor Replay Engine (Checkpoint Resume Mode)..."
	@echo "    ℹ️  Nếu đã chạy trước đó, tiến trình sẽ tự động Resume từ vị trí checkpoint cuối cùng."
	@echo "    ℹ️  Nếu muốn chạy lại từ đầu, hãy dùng: make run-reset"
	docker compose run --rm go-ingestor-replay

## run-kill-0: Replay chi tiết kill events từ file deaths/kill_match_stats_final_0.csv
run-kill-0:
	@echo "[+] Replay Dataset Kill Events: kill_match_stats_final_0.csv..."
	docker compose run --rm -e KAGGLE_SELECTED_FILE=kill_match_stats_final_0.csv go-ingestor-replay

## run-kill-1: Replay chi tiết kill events từ file deaths/kill_match_stats_final_1.csv
run-kill-1:
	@echo "[+] Replay Dataset Kill Events: kill_match_stats_final_1.csv..."
	docker compose run --rm -e KAGGLE_SELECTED_FILE=kill_match_stats_final_1.csv go-ingestor-replay

## run-kill-2: Replay chi tiết kill events từ file deaths/kill_match_stats_final_2.csv
run-kill-2:
	@echo "[+] Replay Dataset Kill Events: kill_match_stats_final_2.csv..."
	docker compose run --rm -e KAGGLE_SELECTED_FILE=kill_match_stats_final_2.csv go-ingestor-replay

## run-kill-3: Replay chi tiết kill events từ file deaths/kill_match_stats_final_3.csv
run-kill-3:
	@echo "[+] Replay Dataset Kill Events: kill_match_stats_final_3.csv..."
	docker compose run --rm -e KAGGLE_SELECTED_FILE=kill_match_stats_final_3.csv go-ingestor-replay

## run-kill-4: Replay chi tiết kill events từ file deaths/kill_match_stats_final_4.csv
run-kill-4:
	@echo "[+] Replay Dataset Kill Events: kill_match_stats_final_4.csv..."
	docker compose run --rm -e KAGGLE_SELECTED_FILE=kill_match_stats_final_4.csv go-ingestor-replay

## run-agg-0: Replay tổng hợp trận đấu từ file aggregate/agg_match_stats_0.csv
run-agg-0:
	@echo "[+] Replay Aggregate Match Stats: agg_match_stats_0.csv..."
	docker compose run --rm -e KAGGLE_SELECTED_FILE=agg_match_stats_0.csv go-ingestor-replay

## run-agg-1: Replay tổng hợp trận đấu từ file aggregate/agg_match_stats_1.csv
run-agg-1:
	@echo "[+] Replay Aggregate Match Stats: agg_match_stats_1.csv..."
	docker compose run --rm -e KAGGLE_SELECTED_FILE=agg_match_stats_1.csv go-ingestor-replay

## run-agg-2: Replay tổng hợp trận đấu từ file aggregate/agg_match_stats_2.csv
run-agg-2:
	@echo "[+] Replay Aggregate Match Stats: agg_match_stats_2.csv..."
	docker compose run --rm -e KAGGLE_SELECTED_FILE=agg_match_stats_2.csv go-ingestor-replay

## run-agg-3: Replay tổng hợp trận đấu từ file aggregate/agg_match_stats_3.csv
run-agg-3:
	@echo "[+] Replay Aggregate Match Stats: agg_match_stats_3.csv..."
	docker compose run --rm -e KAGGLE_SELECTED_FILE=agg_match_stats_3.csv go-ingestor-replay

## run-agg-4: Replay tổng hợp trận đấu từ file aggregate/agg_match_stats_4.csv
run-agg-4:
	@echo "[+] Replay Aggregate Match Stats: agg_match_stats_4.csv..."
	docker compose run --rm -e KAGGLE_SELECTED_FILE=agg_match_stats_4.csv go-ingestor-replay

## sync-kill-0: Materialize kill events từ file deaths/kill_match_stats_final_0.csv lên MinIO
sync-kill-0:
	@echo "[+] Sync Dataset Kill Events: kill_match_stats_final_0.csv..."
	docker compose run --rm -e KAGGLE_SELECTED_FILE=kill_match_stats_final_0.csv go-ingestor-sync

## sync-kill-1: Materialize kill events từ file deaths/kill_match_stats_final_1.csv lên MinIO
sync-kill-1:
	@echo "[+] Sync Dataset Kill Events: kill_match_stats_final_1.csv..."
	docker compose run --rm -e KAGGLE_SELECTED_FILE=kill_match_stats_final_1.csv go-ingestor-sync

## sync-kill-2: Materialize kill events từ file deaths/kill_match_stats_final_2.csv lên MinIO
sync-kill-2:
	@echo "[+] Sync Dataset Kill Events: kill_match_stats_final_2.csv..."
	docker compose run --rm -e KAGGLE_SELECTED_FILE=kill_match_stats_final_2.csv go-ingestor-sync

## sync-kill-3: Materialize kill events từ file deaths/kill_match_stats_final_3.csv lên MinIO
sync-kill-3:
	@echo "[+] Sync Dataset Kill Events: kill_match_stats_final_3.csv..."
	docker compose run --rm -e KAGGLE_SELECTED_FILE=kill_match_stats_final_3.csv go-ingestor-sync

## sync-kill-4: Materialize kill events từ file deaths/kill_match_stats_final_4.csv lên MinIO
sync-kill-4:
	@echo "[+] Sync Dataset Kill Events: kill_match_stats_final_4.csv..."
	docker compose run --rm -e KAGGLE_SELECTED_FILE=kill_match_stats_final_4.csv go-ingestor-sync

## run-parallel: Kích hoạt stream song song liên tục toàn bộ 5 file kill events vào Kafka (Single Container / Multi-R Workers)
run-parallel:
	@echo "[+] Kích hoạt Replay Multi-Stream toàn bộ 5 file kill events (deaths/kill_match_stats_final_0 -> 4)..."
	@echo "    Toàn bộ dữ liệu được đẩy đồng thời vào 6 Kafka Partitions."
	@echo "    Rust Processor (container duy nhất) nhận batch song song và kích hoạt Multi-R Worker Threads!"
	docker compose run --rm -e KAGGLE_SELECTED_FILE=kill_match_stats_final_0.csv go-ingestor-replay & \
	docker compose run --rm -e KAGGLE_SELECTED_FILE=kill_match_stats_final_1.csv go-ingestor-replay & \
	docker compose run --rm -e KAGGLE_SELECTED_FILE=kill_match_stats_final_2.csv go-ingestor-replay & \
	docker compose run --rm -e KAGGLE_SELECTED_FILE=kill_match_stats_final_3.csv go-ingestor-replay & \
	docker compose run --rm -e KAGGLE_SELECTED_FILE=kill_match_stats_final_4.csv go-ingestor-replay & \
	wait
	@echo "  ✓ Phát song song toàn bộ 5 file kill events hoàn tất!"

## bench-kill-parallel: Sync 5 kill CSV thật trước rồi replay song song để benchmark ceiling end-to-end
bench-kill-parallel:
	@echo "[+] Materialize 5 kill CSV thật trước khi chạy benchmark song song..."
	@$(MAKE) sync-kill-0
	@$(MAKE) sync-kill-1
	@$(MAKE) sync-kill-2
	@$(MAKE) sync-kill-3
	@$(MAKE) sync-kill-4
	@$(MAKE) run-parallel

## run-reset: Xóa Checkpoint cũ và phát lại toàn bộ dữ liệu từ đầu (dòng 1)
run-reset:
	@echo "[+] 🔄 Xóa Checkpoint cũ, phát lại toàn bộ CSV từ dòng 1..."
	# Override command của service go-ingestor-replay thêm cờ -reset-checkpoint=true
	docker compose run --rm go-ingestor-replay \
		/app/bin/replay -limit=0 -stream-delay-ms=0 -dry-run=false -disable-checkpoint=false -reset-checkpoint=true
	@echo "  ✓ Reset hoàn tất, phát lại dữ liệu từ đầu thành công!"

## stop: Tạm dừng toàn bộ các containers đang chạy (bảo toàn volume dữ liệu)
stop:
	@echo "[+] Tạm dừng toàn bộ các containers (giữ nguyên data volume)..."
	docker compose stop

## down: Dừng và giải phóng container hạ tầng
down: stop

## restart: Tái khởi động lại toàn bộ containers
restart:
	@echo "[+] Tái khởi động lại toàn bộ stack containers..."
	docker compose restart

## purge: Dừng container, xóa toàn bộ volume Docker và làm sạch dữ liệu rác (Zero-State Clean)
purge:
	@echo "[+] Dừng và dọn dẹp triệt để Docker Containers & Data Volumes..."
	docker compose down -v --remove-orphans
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

## verify-invariants: Kiểm tra 7 Định luật Bảo toàn Dữ liệu trên Data Lake S3
verify-invariants:
	@echo "[+] Kiểm tra 7 Định luật Bảo toàn Dữ liệu trên MinIO Data Lake S3..."
	@python3 scripts/verify_invariants.py

## clean: Dọn dẹp các file build tạm và log
clean:
	@echo "[+] Dọn dẹp dữ liệu tạm và file build..."
	@rm -rf bin/ target/ tmp/ *.log
	@find . -name "*.tmp" -type f -delete
	@echo "  - Đã hoàn tất dọn dẹp."

## logs: Theo dõi log thời gian thực từ tất cả các container hạ tầng
logs:
	docker compose logs -f

## logs-ml: Theo dõi log thời gian thực của Python ML Worker & Inference Engine
logs-ml:
	docker compose logs -f ml-platform
