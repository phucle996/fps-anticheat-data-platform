# PUBG Anti-Cheat Data Platform

Hệ thống xử lý và phân tích dữ liệu gian lận game PUBG end-to-end theo kiến trúc Cloud-Native & Event-Driven Streaming Data Platform.

---

## 1. Kiến trúc Tổng quan (Architecture Overview)

Dữ liệu tĩnh từ Kaggle PUBG Dataset được stream thời gian thực qua Kafka, xử lý Gom Batch hiệu năng cao bằng Rust để lưu vào MinIO Data Lake (Parquet format). Hệ thống R Analytics thực thi Feature Engineering, Anomaly Detection ML Model (Isolation Forest) và hiển thị kết quả trên Shiny Dashboard.

```text
Kaggle CSV  --->  Go Ingestor  --->  Kafka  --->  Rust Batch Processor  --->  MinIO Data Lake  --->  R Analytics ML  --->  Shiny Dashboard
```

Chi tiết kiến trúc hệ thống tham khảo tại document [ARCH.md](ARCH.md).

---

## 2. Cấu trúc Monorepo (Repository Structure)

```text
fps-anticheat/
├── apps/                        # Mã nguồn các dịch vụ độc lập
│   ├── go-ingestor/             # Tải & Replay dữ liệu Kaggle vào Kafka (Go)
│   ├── rust-processor/          # Tiêu thụ Kafka, gom batch & ghi Parquet xuống MinIO (Rust)
│   ├── r-pipeline/              # Preprocessing, Feature Store & ML Anomaly Detection (R)
│   └── shiny-dashboard/         # Giao diện Web hiển thị phân tích rủi ro & bằng chứng (Shiny)
├── contracts/                   # Data Contracts chung giữa các service (JSON Schemas)
│   ├── schemas/                 # Event Envelope, Manifest & Prediction JSON Schemas
│   ├── examples/                # File JSON mẫu cho Event hợp lệ / lỗi
│   └── data-dictionary/         # Từ điển dữ liệu giải thích chỉ số game
├── configs/                     # Cấu hình môi trường (local, development, production)
├── data/                        # Thư mục lưu tạm dữ liệu local (archives, extracted, manifests)
├── deployments/                 # Docker Compose & Kubernetes Deployment Manifests
│   └── compose/init/            # Scripts khởi tạo Kafka Topics & MinIO Buckets
├── scripts/                     # Utility scripts cho Dataset, Kafka, MinIO, Demo
└── tests/                       # Integration & End-to-End Test suites
```

---

## 3. Khởi tạo & Hướng dẫn Nhanh (Quick Start)

### Yêu cầu Tiền đề (Prerequisites)
- **Go** >= 1.22
- **Rust (Cargo)** >= 1.75
- **R** >= 4.3 & Package `renv`
- **Docker & Docker Compose**

### Các Bước Khởi tạo
1. **Khởi tạo file cấu hình môi trường**:
   ```bash
   make init
   ```
2. **Kiểm tra công cụ hệ thống**:
   ```bash
   make check-deps
   ```
3. **Theo dõi tiến độ phát triển**:
   Tham khảo danh mục công việc chi tiết tại [checklist.md](checklist.md).
