# 🛡️ FPS Anti-Cheat Data Platform

Hệ thống xử lý và phân tích dữ liệu gian lận game PUBG end-to-end theo kiến trúc **Cloud-Native, High-Availability (HA) & Event-Driven Streaming Data Platform**.

Dữ liệu tĩnh từ Kaggle được stream qua Kafka, xử lý batch hiệu năng cao bằng Rust để ghi vào Data Lake (MinIO), phân tích bằng R, huấn luyện mô hình ML (Python + ONNX), suy luận siêu tốc bằng Rust, và hiển thị kết quả trên Streamlit Dashboard.

---

## 1. 🏗️ Kiến Trúc Tổng Quan (Architecture Overview)

```text
+==================================================================================================+
|                        FPS ANTI-CHEAT DATA PLATFORM — E2E ARCHITECTURE                           |
+==================================================================================================+

  ┌──────────────────────┐       ────── HTTPS API ──────>      ┌─────────────────────────────────┐
  │ Kaggle PUBG Dataset  │                                     │     Go Ingestor (Go 1.26)      │
  │ (PUBG Telemetry CSVs)│                                     │ • Normalize & Validate Schema   │
  └──────────────────────┘                                     │ • Micro-Batch (20msg/16KB/500ms)│
                                                               │ • StreamDelay (real-time sim)   │
                                                               │ • Checkpoint Resume (MinIO S3)  │
                                                               └────────────────┬────────────────┘
                                                                                │
                                                                    JSON Events │ (Key: match_id)
                                                                                v
                                                               ┌─────────────────────────────────┐
                                                               │  Apache Kafka (KRaft 7.6.1)     │
                                                               │  • raw topic (Partitions: 3)    │
                                                               │  • invalid topic (DLQ)          │
                                                               └────────────────┬────────────────┘
                                                                                │ Consumer Group
                                                                                v
  ┌────────────────────────────┐                                     ┌───────────────────────────────────┐
  │ MinIO S3 Data Lake         │                                     │ Rust Processor Container (Rust)   │
  │ • Bronze (Raw Parquet)     │                                     │ • Consume Kafka -> Validate Rules │
  │ • Silver (Cleaned Entities)│      <── Parquet + Manifest ──      │ • Arrow & Parquet (Zstd 4.2x)     │
  │ • Gold (Feature Matrix)    │                                     │ • Durable Two-Phase Commit (2PC)  │
  │ • Models & Predictions     │                                     │ ┌───────────────────────────────┐ │
  └─────────────┬──────────────┘                                     │ │ R Worker Subprocess (R 4.4)   │ │
                │                                                    │ │ • daemon_worker.R (5s idle)   │ │
                │                                                    │ └───────────────────────────────┘ │
                │                                                    └─────────────────┬─────────────────┘
                │                                                                      │
                │ Gold Features S3 Read                                 Kafka Signal:  │ gold.ready
                v                                                                      v
  ┌──────────────────────────────────────────────────────────────────────────────────────────────────────┐
  │ ML Platform 3-in-1 Ecosystem (apps/ml-platform)                                                      │
  │                                                                                                      │
  │ ┌─────────────────────────────────┐   ────── Export ONNX ──────>   ┌───────────────────────────────┐ │
  │ │ Python ML Worker (Python 3.13)  │                                │ Rust Inference Engine (Rust)  │ │
  │ │ • Train IsoForest & XGBoost     │   Save to s3://pubg-models/    │ • ONNX Runtime + Hot-Swap RAM │ │
  │ │ • Export 6-input ONNX Model     │   Signal: model.ready          │ • Robust Z-Score Evidence Gen │ │
  │ └─────────────────────────────────┘                                └───────────────┬───────────────┘ │
  │                                                                                    │                 │
  │                                                                       Unix Socket  │ IPC Request/Resp│
  │                                                                       /tmp/*.sock  │ (< 0.1ms)       │
  │                                                                                    v                 │
  │                                                                    ┌───────────────────────────────┐ │
  │                                                                    │ Go REST API Gateway (Go 1.26) │ │
  │                                                                    │ • GET /api/v1/health & summary│ │
  │                                                                    │ • POST /api/v1/predict (UDS)  │ │
  │                                                                    └───────────────┬───────────────┘ │
  └─────────────┬──────────────────────────────────────────────────────────────────────┼─────────────────┘
                │                                                                      │
                │ S3 Read (Silver / Gold / Predictions)                                │ HTTP REST API Client
                v                                                                      v
  ┌──────────────────────────────────────────────────────────────────────────────────────────────────────┐
  │ Streamlit Risk Analysis & Telemetry Dashboard (Python 3.13 / Streamlit, Port 8501)                  │
  │ Views: Overview (KPI Stats)  |  Data Quality & Preprocessing  |  Player Analysis  |  Risk Fraud     │
  └──────────────────────────────────────────────────────────────────────────────────────────────────────┘
```

---

## 2. 📂 Cấu Trúc Monorepo (Repository Structure)

```text
fps-anticheat/
├── apps/                             # Microservices & Processors
│   ├── go-ingestor/                  # [Go 1.26] Nạp & stream CSV vào Kafka
│   ├── rust-processor/               # [Rust 1.87] Stream Processing + Parquet S3 + R Worker Pool
│   │   └── r-processor/              #   [R 4.4] Subprocess ETL: Bronze -> Silver -> Gold + EDA
│   ├── ml-platform/                  # ML Platform tích hợp 3-in-1:
│   │   ├── python-ml-worker/         #   [Python 3.13] Train ML Model + Export ONNX
│   │   ├── rust-inference/           #   [Rust 1.87] ONNX Inference Engine + UDS IPC
│   │   └── go-api/                   #   [Go 1.26] REST API Gateway
│   └── streamlit-dashboard/          # [Python 3.13 / Streamlit] UI Dashboard
├── contracts/                        # Data Contracts chung (JSON Schemas)
│   ├── schemas/                      # Event Envelope, Manifest, Prediction Schemas
│   ├── examples/                     # Sample valid/invalid events
│   └── data-dictionary/              # Từ điển giải thích các chỉ số game
├── configs/                          # Cấu hình môi trường (local, dev, production)
├── deployments/                      # Docker Compose & K8s manifests
│   └── compose/                      # docker-compose.yml + init scripts
├── docs/                             # Tài liệu kiến trúc & Test Suites
│   ├── DATA_LAKE_LAYOUT.md           # Quy chuẩn Medallion Data Lake
│   ├── DOCKER_INTEGRATION_TEST_SUITE.md
│   └── EDA_REPORT.md
├── scripts/                          # Shell scripts setup & testing
├── docker-compose.yml                # Root wrapper -> deployments/compose/
├── Makefile                          # Multi-language monorepo commands
├── ARCH.md                           # Tài liệu kiến trúc chi tiết
└── checklist.md                      # Roadmap & tiến độ dự án
```

---

## 3. 📡 Kafka Topic Registry

| Topic | Partitions | Retention | Producer | Consumer | Mô Tả |
|:---|:---:|:---:|:---|:---|:---|
| `pubg.v1.player-stat.raw` | 3 | 7 ngày | Go Ingestor | Rust Processor | Event Envelope hợp lệ, key = `match_id` |
| `pubg.v1.invalid` | 1 | 30 ngày | Go Ingestor | (Audit) | Dead-Letter Queue bản ghi lỗi validation |
| `pubg.v1.dataset.gold.ready` | — | — | Rust Processor | Python ML Worker | Signal khi Gold features sẵn sàng |
| `pubg.v1.ml.model.ready` | — | — | Python ML Worker | Rust Inference | Signal khi model ONNX mới được export |

---

## 4. 💾 MinIO Data Lake Layout (Medallion)

```text
fps-anticheat-datalake/
├── bronze/                           # Raw Ingestion Layer
│   ├── player-stat/                  # Parquet files từ Rust Processor
│   └── invalid/                      # JSON bản ghi lỗi Data Quality
├── manifests/                        # Batch Manifest audit logs (JSON)
├── silver/                           # Cleaned & Modeled Entities
│   ├── players/                      # Player profiles
│   ├── matches/                      # Match summaries
│   └── player-match/                 # Player-Match detailed stats
├── gold/                             # Feature Store cho ML
│   └── player-match-features/        # Feature Matrix (Parquet)
├── models/                           # ML Model Artifacts (ONNX, metadata)
└── predictions/                      # Anomaly Detection scoring results
```

> Chi tiết Naming Convention & Hive Partitioning: [DATA_LAKE_LAYOUT.md](docs/DATA_LAKE_LAYOUT.md)

---

## 5. 🔗 Danh Mục Dịch Vụ & README Chi Tiết

| # | Service | Tech Stack | README |
|:---:|:---|:---|:---|
| 1 | **Go Ingestor** | Go 1.26, Kafka, MinIO | [apps/go-ingestor/README.md](apps/go-ingestor/README.md) |
| 2 | **Rust Stream Processor** | Rust 1.87, rdkafka, Arrow, Parquet | [apps/rust-processor/README.md](apps/rust-processor/README.md) |
| 3 | **R Processor** | R 4.4, arrow, dplyr | *(Subprocess của Rust — xem `apps/rust-processor/r-processor/R/`)* |
| 4 | **ML Platform (3-in-1)** | Python 3.13, Rust 1.87, Go 1.26 | [apps/ml-platform/README.md](apps/ml-platform/README.md) |
| 5 | **Streamlit Dashboard** | Python 3.13, Streamlit | *(chưa có README — xem `apps/streamlit-dashboard/app.py`)* |

### Tài Liệu Bổ Sung

| Tài liệu | Đường dẫn |
|:---|:---|
| Kiến trúc chi tiết (ARCH) | [ARCH.md](ARCH.md) |
| Data Lake Layout & Naming | [docs/DATA_LAKE_LAYOUT.md](docs/DATA_LAKE_LAYOUT.md) |
| Docker Integration Test Suite (42 Cases) | [docs/DOCKER_INTEGRATION_TEST_SUITE.md](docs/DOCKER_INTEGRATION_TEST_SUITE.md) |
| EDA Report | [docs/EDA_REPORT.md](docs/EDA_REPORT.md) |
| Checklist & Roadmap | [checklist.md](checklist.md) |

---

## 6. 🚀 Quick Start

### Yêu cầu
- **Go** ≥ 1.26
- **Rust (Cargo)** ≥ 1.87 (Edition 2024)
- **Python** ≥ 3.13
- **R** ≥ 4.4
- **Docker & Docker Compose** (BuildKit enabled)

### Khởi chạy hạ tầng

```bash
# Khởi chạy toàn bộ (Kafka KRaft + MinIO + Init Containers + Services)
docker compose up -d

# Hoặc chỉ hạ tầng cơ sở
docker compose -f deployments/compose/docker-compose.yml up -d kafka minio init-kafka init-minio
```

### Lệnh Makefile hữu ích

```bash
make help         # Liệt kê tất cả các lệnh khả dụng
make check-deps   # Kiểm tra Go, Rust, R, Python3, Docker
make init         # Khởi tạo file .env, bật containers & tạo S3 Buckets / Kafka Topics
make start        # Khởi chạy toàn bộ các containers hạ tầng
make run          # Thực thi kịch bản Runbook End-to-End với data thực tế từ Kaggle CSV
make stop         # Tạm dừng toàn bộ containers (bảo toàn volume dữ liệu)
make restart      # Tái khởi động lại toàn bộ containers
make purge        # Dọn dẹp triệt để containers, Docker volumes & file tạm (Zero-State)
make test         # Chạy unit tests cho tất cả 4 ngôn ngữ (Go, Rust, Python, Streamlit)
make fmt          # Format code toàn bộ monorepo (Go, Rust)
make logs         # Theo dõi log thời gian thực từ containers
```

---

## 📋 Tiến Độ Công Việc

Tham khảo chi tiết tại [checklist.md](checklist.md).
