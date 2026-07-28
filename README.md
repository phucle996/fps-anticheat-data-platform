# 🛡️ PUBG Anti-Cheat Data Platform

Hệ thống xử lý và phân tích dữ liệu gian lận game PUBG end-to-end theo kiến trúc **Cloud-Native, High-Availability (HA) & Event-Driven Streaming Data Platform**.

---

## 1. 🏗️ Kiến trúc Tổng quan (Architecture Overview)

Hệ thống được thiết kế theo mô hình **Medallion Data Lake Architecture** (Bronze $\rightarrow$ Silver $\rightarrow$ Gold) kết hợp với **Durable Two-Phase Commit (2PC)** và cơ chế **Fail-Close 100%** bảo mật tuyệt đối.

```text
  [ Kaggle CSV Dataset ]
           │
           ▼
  [ Go Ingestor Service ] (Go 1.26)
           │ (Publish JSON Event Envelopes)
           ▼
  [ Kafka Event Stream ] (cp-kafka 7.6.1 KRaft)
           │
           ▼
  [ Rust Processor Engine ] (Rust 1.87, Edition 2024)
     ├── 11 Semantic Rules Validation & Deduplication
     ├── Arrow Columnar / Parquet Zstd Serialization
     ├── MinIO S3 Bronze Layer Storage
     └── Spark-Style Dynamic R Worker Pool Dispatch
           │
           ├───> [ MinIO S3 Data Lake ] (Medallion: Bronze / Silver / Gold / Manifests)
           │
           ├───> [ R Feature Engine ] ──> Silver & Gold Parquet Feature Store
           │
           ├───> [ Python ML Worker ] ─> Isolation Forest Training ──> ONNX Model Export
           │                                                                │
           │                                                                ▼
           ├───> [ Rust Inference Engine ] <── (UDS IPC / ONNX Runtime) ────┘
           │
           ├───> [ Go REST API Gateway ] (Go 1.26 REST Endpoints)
           │
           └───> [ Streamlit Risk Dashboard ] (Python 3.13 Risk Score Evidence UI)
```

### Các Thành phần Dịch vụ (Core Services)

| Dịch vụ | Công nghệ | Chức năng chính |
|---|---|---|
| **`go-ingestor`** | Go 1.26 | Stream dữ liệu Kaggle CSV, validate Data Contract, publish sang Kafka topic `pubg.v1.player-stat.raw`. |
| **`pubg-kafka`** | Kafka 7.6.1 (KRaft) | Message broker phân tán high-throughput, quản lý partition offset và event log. |
| **`pubg-minio`** | MinIO S3 | Medallion Data Lake Storage (9 thư mục móng Bronze, Silver, Gold, Manifests, Models). |
| **`rust-processor`** | Rust 1.87 (Edition 2024) | Consume Kafka, validate 11 Semantic Rules, Dedup, chuyển đổi Arrow/Parquet Zstd, ghi Bronze S3, Durable 2PC commit. |
| **`r-processor`** | R 4.4 | Spark-style Dynamic Worker Pool tính toán Silver/Gold features và huấn luyện mô hình Isolation Forest. |
| **`python-ml-worker`** | Python 3.13 | ML Pipeline, huấn luyện mô hình gian lận, export ONNX model. |
| **`rust-inference`** | Rust 1.87 | IPC Server giao tiếp qua Unix Domain Socket (`/tmp/rust_inference.sock`) thực thi ONNX inference siêu tốc. |
| **`go-api`** | Go 1.26 | High-concurrency REST API Gateway cung cấp endpoint truy vấn chỉ số và Risk Score người chơi. |
| **`streamlit-dashboard`** | Python 3.13 / Streamlit | Dashboard tương tác xem tổng quan rủi ro, phân tích người chơi và bằng chứng gian lận (Top Evidence Features). |

---

## 2. 📂 Cấu trúc Monorepo (Repository Structure)

```text
fps-anticheat/
├── apps/                        # Các microservices độc lập
│   ├── go-ingestor/             # Service nạp & stream dữ liệu vào Kafka (Go 1.26)
│   ├── rust-processor/          # Engine xử lý stream, validation & Parquet S3 (Rust 1.87)
│   ├── r-processor/             # R Feature Engine & Dynamic Worker Pool (R)
│   ├── ml-platform/             # Python ML Worker, Go API & Rust Inference Engine
│   └── streamlit-dashboard/     # UI Dashboard hiển thị phân tích bằng chứng rủi ro (Streamlit)
├── contracts/                   # Data Contracts chung giữa các service (JSON Schemas)
│   ├── schemas/                 # Event Envelope, Manifest & Prediction Schemas
│   └── data-dictionary/         # Từ điển giải thích các chỉ số game PUBG
├── docs/                        # Tài liệu thiết kế & Bộ 42 Docker Integration Test Cases
│   └── DOCKER_INTEGRATION_TEST_SUITE.md
├── deployments/                 # Docker Compose & Kubernetes Deployment Manifests
│   └── compose/                 # Docker Compose file & cấu hình Kafka KRaft / MinIO
├── scripts/                     # Utility scripts (Init Data Lake, Runner integration tests)
└── checklist.md                 # Roadmap & tiến độ hoàn thành các Phase của dự án
```

---

## 3. 🚀 Hướng Dẫn Vận Hành & Khởi Chạy (Quick Start)

### Yêu cầu Tiền đề (Prerequisites)
- **Go** $\ge$ 1.26
- **Rust (Cargo)** $\ge$ 1.87 (Edition 2024)
- **Python** $\ge$ 3.13
- **Docker & Docker Compose** (BuildKit enabled)

---

### Bước 1: Khởi chạy Hạ tầng Container (Kafka & MinIO)

```bash
# Khởi chạy Kafka KRaft Mode và MinIO S3 Container
docker compose -f deployments/compose/docker-compose.yml up -d
```

Kiểm tra sức khỏe các containers:
```bash
docker compose -f deployments/compose/docker-compose.yml ps
```

---

### Bước 2: Build Docker Images Các Microservices

```bash
# Build Go Ingestor Image (Go 1.26)
docker build -t pubg-go-ingestor:latest apps/go-ingestor/

# Build Rust Stream Processor Image (Rust 1.87)
docker build -t pubg-rust-processor:latest apps/rust-processor/
```

---

### Bước 3: Chạy Bộ 42 Integration Test Cases trên Docker

Dự án cung cấp bộ kiểm thử tích hợp toàn diện 42 Test Cases bao phủ 8 Domains (Fail-Close, Data Quality, Checkpointing, Circuit Breaker, Durable 2PC,...).

Chạy thử nghiệm **Domain 1 (Fail-Close & Configuration Resilience)**:
```bash
# Chạy script tự động kiểm thử Domain 1
./scripts/run_domain1_tests.sh
```

Hoặc chạy thủ công kiểm thử cơ chế **Fail-Close** của `go-ingestor`:
```bash
docker run --rm --network pubg-platform-net \
  -e KAFKA_BROKERS="" \
  -e MINIO_ENDPOINT="http://minio:9000" \
  pubg-go-ingestor:latest
```
*Kết quả kỳ vọng*: Container ném lỗi `FATAL: phát hiện 1 biến môi trường chưa khai báo: [KAFKA_BROKERS]` và ngắt ngay lập tức với **Exit Code 1**.

Chi tiết bộ 42 Test Cases tham khảo thêm tại [docs/DOCKER_INTEGRATION_TEST_SUITE.md](docs/DOCKER_INTEGRATION_TEST_SUITE.md).

---

## 📋 Danh Mục Tiến Độ Công Việc

Tham khảo danh mục công việc chi tiết và tình trạng hoàn thành các Phase tại [checklist.md](checklist.md).
