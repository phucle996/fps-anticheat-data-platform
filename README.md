# 🛡️ FPS Anti-Cheat Data Platform

Hệ thống xử lý và phân tích dữ liệu gian lận game PUBG end-to-end theo kiến trúc **Cloud-native-inspired Event-Driven Streaming Data Platform** (môi trường local/dev).

Dữ liệu tĩnh từ Kaggle được stream qua Kafka, xử lý batch hiệu năng cao bằng **100% Native Rust (Apache Arrow & Parquet)** để ghi vào Data Lake (MinIO), huấn luyện mô hình ML tăng tốc phần cứng **NVIDIA GPU CUDA** (Python + ONNX), suy luận bằng Rust ONNX Engine, và hiển thị kết quả trên Streamlit Dashboard.

---

## 1. 🏗️ Kiến Trúc Tổng Quan (Architecture Overview)

```text
+==================================================================================================+
|                        FPS ANTI-CHEAT DATA PLATFORM — E2E ARCHITECTURE                           |
+==================================================================================================+

  ┌──────────────────────┐       ────── HTTPS API ──────>      ┌─────────────────────────────────┐
  │ Kaggle PUBG Dataset  │                                     │     Go Ingestor (Go 1.26)      │
  │ (kill_match_stats    │                                     │ • Normalize & Validate Schema   │
  │  _final_0.csv ~1.9M) │                                     │ • Micro-Batch (500msg/64KB/500ms)│
  │                      │                                     │ • Checkpoint Resume (MinIO S3)  │
  │                      │                                     │ • Graceful Shutdown SIGINT/⚡   │
  └──────────────────────┘                                     └────────────────┬────────────────┘
                                                                                │
                                                                    JSON Events │ (Key: match_id)
                                                                                v
                                                               ┌─────────────────────────────────┐
                                                               │  Apache Kafka (KRaft 7.6.1)     │
                                                               │  • raw topic (Partitions: 6)    │
                                                               │  • invalid topic (DLQ)          │
                                                               └────────────────┬────────────────┘
                                                                                │ Consumer Group
                                                                                v
  ┌────────────────────────────┐                                     ┌───────────────────────────────────┐
  │ MinIO S3 Data Lake         │                                     │ Native Rust Processor (Rust 1.88) │
  │ • Bronze (Raw Parquet)     │      <── Parquet + Manifest ──      │ • Consume Kafka -> Validate Rules │
  │ • Silver (Cleaned Entities)│                                     │ • Native Arrow & Parquet (Zstd)   │
  │ • Gold (Feature Matrix)    │                                     │ • In-Process Parallel Worker Pool │
  │ • Models & Predictions     │                                     │ • Pure Native Rust (100% R Purged)│
  └─────────────┬──────────────┘                                     └─────────────────┬─────────────────┘
                │                                                                      │
                │ Gold Features S3 Read                                 Kafka Signal:  │ gold.ready
                v                                                                      v
  ┌──────────────────────────────────────────────────────────────────────────────────────────────────────┐
  │ ML Platform 3-in-1 Ecosystem (apps/ml-platform)                                                      │
  │                                                                                                      │
  │ ┌─────────────────────────────────┐   ────── Export ONNX ──────>   ┌───────────────────────────────┐ │
  │ │ Python ML Worker (Python 3.13)  │                                │ Rust Inference Engine (Rust)  │ │
  │ │ • Supervised XGBoost CUDA GPU   │   Save to s3://pubg-models/    │ • ONNX Runtime CUDA Provider  │ │
  │ │ • Export 5-feature ONNX Model   │   Signal: model.ready          │ • Pure GPU Acceleration Engine│ │
  │ └─────────────────────────────────┘                                └───────────────┬───────────────┘ │
  │                                                                                    │                 │
  │                                                                       Unix Socket  │ IPC Request/Resp│
  │                                                                       /tmp/*.sock  │ (low-latency)   │
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
│   ├── rust-processor/               # [Rust 1.88] Native Stream Processing + Arrow/Parquet S3 (~40MB Image)
│   ├── ml-platform/                  # ML Platform tích hợp 3-in-1 (GPU Accelerated):
│   │   ├── python-ml-worker/         #   [Python 3.13] Train XGBoost (CUDA GPU) + Export ONNX
│   │   ├── rust-inference/           #   [Rust 1.88] ONNX Inference Engine (CUDA Provider) + UDS IPC
│   │   └── go-api/                   #   [Go 1.26] REST API Gateway
│   └── streamlit-dashboard/          # [Python 3.13 / Streamlit] UI Dashboard
├── contracts/                        # Data Contracts chung (JSON Schemas)
│   ├── schemas/                      # Event Envelope, Manifest, Prediction Schemas
│   ├── examples/                     # Sample valid/invalid events
│   └── data-dictionary/              # Từ điển giải thích các chỉ số game
├── configs/                          # Cấu hình môi trường (local, dev, production)
├── init/                             # Script khởi tạo Kafka topics & MinIO buckets
├── docs/                             # Tài liệu kiến trúc & Test Suites
│   ├── DATA_LAKE_LAYOUT.md           # Quy chuẩn Medallion Data Lake
│   ├── DOCKER_INTEGRATION_TEST_SUITE.md
│   └── EDA_REPORT.md
├── scripts/                          # Shell scripts setup & testing
├── docker-compose.yml                # Docker Compose chính (NVIDIA GPU Passthrough Enabled)
├── Makefile                          # Multi-language monorepo commands
├── ARCH.md                           # Tài liệu kiến trúc chi tiết
└── checklist.md                      # Roadmap & tiến độ dự án
```

---

## 3. 📡 Kafka Topic Registry

| Topic | Partitions | Retention | Producer | Consumer | Mô Tả |
|:---|:---:|:---:|:---|:---|:---|
| `pubg.v1.player-stat.raw` | 6 | 24h | Go Ingestor | Rust Processor | Event Envelope hợp lệ (match_summary + kill_event), key = `match_id` |
| `pubg.v1.kill-event.raw` | 6 | 24h | Go Ingestor | Rust Processor | Kill events riêng (schema match_deaths) |
| `pubg.v1.invalid` | 3 | 30 ngày | Go Ingestor | (Audit) | Dead-Letter Queue bản ghi lỗi validation |
| `pubg.v1.dataset.gold.ready` | 1 | 24h | Native Rust Processor | Python ML Worker | Signal khi Gold Parquet batch sẵn sàng |
| `pubg.v1.ml.model.ready` | 1 | 7 ngày | Python ML Worker | Rust Inference | Signal khi ONNX model version mới được upload |

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

---

## 5. 🔗 Danh Mục Dịch Vụ & README Chi Tiết

| # | Service | Tech Stack | README |
|:---:|:---|:---|:---|
| 1 | **Go Ingestor** | Go 1.26, Kafka, MinIO | [apps/go-ingestor/README.md](apps/go-ingestor/README.md) |
| 2 | **Native Rust Stream Processor** | Rust 1.88, Apache Arrow 52, Parquet | [apps/rust-processor/README.md](apps/rust-processor/README.md) |
| 3 | **ML Platform (3-in-1)** | Python 3.13 (XGBoost CUDA), Rust 1.88 (ONNX CUDA), Go 1.26 | [apps/ml-platform/README.md](apps/ml-platform/README.md) |
| 4 | **Streamlit Dashboard** | Python 3.13, Streamlit | *(xem `apps/streamlit-dashboard/app.py`)* |

---

## 6. 🚀 Quick Start

### Yêu cầu Tiền Trạm (Prerequisites)
- **Go** ≥ 1.26
- **Rust (Cargo)** ≥ 1.88
- **Python** ≥ 3.13
- **NVIDIA GPU & Drivers** (`nvidia-smi` hoạt động tốt)
- **NVIDIA Container Toolkit** (`nvidia-ctk` đã được cấu hình cho Docker runtime)
- **Docker & Docker Compose** (BuildKit enabled)

### Khởi chạy hạ tầng

```bash
# Khởi tạo toàn bộ (.env, S3 Buckets, Kafka Topics, Kaggle Sync & Services)
make init

# Hoặc chạy Docker Compose trực tiếp
docker compose up -d
```

### Lệnh Makefile hữu ích

```bash
make help         # Liệt kê tất cả các lệnh khả dụng
make check-deps   # Kiểm tra Go, Rust, Python3, Docker
make init         # Khởi tạo file .env, bật containers & tạo S3 Buckets / Kafka Topics / tải Dataset Kaggle lên MinIO
make start        # Khởi chạy toàn bộ các containers hạ tầng
make run          # Stream Replay liên tục (Auto-Resume từ Checkpoint MinIO S3 nếu đã ngắt)
make run-reset    # Xóa Checkpoint cũ, phát lại toàn bộ CSV từ dòng 1
make stop         # Tạm dừng toàn bộ containers (bảo toàn volume dữ liệu)
make restart      # Tái khởi động lại toàn bộ containers
make purge        # Dọn dẹp triệt để containers, Docker volumes & file tạm (Zero-State)
make test         # Chạy unit tests cho tất cả ngôn ngữ
make fmt          # Format code toàn bộ monorepo (Go, Rust)
make logs         # Theo dõi log thời gian thực từ containers
```

---

## 📋 Tiến Độ Công Việc

Tham khảo chi tiết tại [checklist.md](checklist.md).
