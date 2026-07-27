# Unified Machine Learning Platform (`apps/ml-platform`)

Thư mục **`apps/ml-platform/`** chứa hệ sinh thái 3 dịch vụ vi mô (3-in-1 Microservices Architecture) chịu trách nhiệm huấn luyện mô hình Machine Learning, thực thi suy luận AI siêu tốc và cung cấp cổng REST API phục vụ phát hiện gian lận real-time cho nền tảng **PUBG PC Anti-Cheat Data Platform**.

---

## 🏛️ Kiến Trúc 3-trong-1 (3-in-1 Unified Architecture)

```text
[ Go REST API Gateway ] ◄──(HTTP REST /api/v1/predict)──► [ Clients / Streamlit ]
          │
          │ (Unix Domain Socket IPC: /tmp/rust_inference.sock - Latency < 0.1ms)
          ▼
[ Rust ONNX Inference Engine ] ──(Đọc trực tiếp model.onnx)──► [ Shared Model Storage: models/v1/ ]
          ▲                                                                   ▲
          │ (UDS Notification Signal)                                         │ (Export ONNX)
          └─── [ Python ML Training Engine ] ─────────────────────────────────┘
```

---

## 🚀 Danh Sách Dịch Vụ Con (Core Sub-Services)

### 1. `apps/ml-platform/python-ml-worker`
- **Chức năng**: Huấn luyện các mô hình Machine Learning (Random Forest, HistGradientBoosting, Isolation Forest) trên tập đặc trưng Gold Features.
- **ONNX Model Exporter**: Xuất mô hình sang định dạng chuẩn ONNX 6-inputs (`kills_pm`, `damage_pm`, `headshot_ratio`, `damage_per_kill`, `movement_pm`, `perf_vs_lobby`).
- **Signal**: Gửi tín hiệu thông báo cho Rust Inference Engine qua IPC khi xuất xong mô hình mới.

### 2. `apps/ml-platform/rust-inference`
- **Chức năng**: Engine suy luận ONNX bằng Rust thuần sử dụng `ort` crate kết hợp Atomic Hot-Swap RAM (`Arc<RwLock<LoadedModel>>`) đảm bảo Zero Downtime.
- **UDS IPC Server**: Lắng nghe tại Unix Domain Socket `/tmp/rust_inference.sock` nhận request JSON và trả về Anomaly Risk Score (0.0 - 1.0) & Risk Level (`LOW`, `MEDIUM`, `HIGH`, `CRITICAL`).
- **Evidence Matrix Generator**: Tính toán chỉ số Robust Z-Score ($Z_{robust} = \frac{X_i - \text{Median}}{\text{MAD} \times 1.4826}$) và trích xuất bằng chứng gian lận nổi bật nhất.
- **Parquet Storage Writer**: Đóng gói toàn bộ kết quả suy luận ghi thành file Parquet lưu trữ lên MinIO Data Lake (`s3://pubg-predictions/`).

### 3. `apps/ml-platform/go-api`
- **Chức năng**: Cổng HTTP REST API Gateway bảo mật phục vụ các ứng dụng Frontend và Streamlit Dashboard.
- **Endpoints**:
  - `GET /api/v1/health`: Kiểm tra liveness và kết nối file socket UDS IPC.
  - `POST /api/v1/predict`: Nhận payload dự báo và chuyển tiếp tới Rust Engine qua UDS IPC.
  - `GET /api/v1/dataset/summary`: Cung cấp 10 chỉ số KPI thống kê dữ liệu cho Streamlit Dashboard.

---

## ⚙️ Tập Cấu Hình Môi Trường Chung (`apps/ml-platform/.env`)

Tất cả 3 dịch vụ trong ML Platform chia sẻ chung file cấu hình `.env` với nguyên tắc **Fail-Close 100% (Zero Default Fallback)**:

```ini
# Cấu hình Go API Gateway
HTTP_PORT=8081
IPC_SOCKET_PATH=/tmp/rust_inference.sock

# Cấu hình MinIO S3 Object Storage
MINIO_ENDPOINT=http://localhost:9000
MINIO_BUCKET_DATA=fps-anticheat-datalake
MINIO_BUCKET_MODEL=pubg-models
MINIO_ACCESS_KEY=minioadmin
MINIO_SECRET_KEY=minioadmin

# Cấu hình Kafka Message Broker
KAFKA_BROKERS=localhost:9092
KAFKA_TOPIC_GOLD=pubg.v1.dataset.gold.ready
KAFKA_TOPIC_MODEL=pubg.v1.ml.model.ready
```

---

## 🛠️ Hướng Dẫn Kiểm Thử & Chạy Dịch Vụ (Commands)

### 1. Kiểm thử Rust Inference Engine & UDS IPC & Evidence Matrix
```bash
cd apps/ml-platform/rust-inference
cargo test -- --nocapture
```

### 2. Kiểm thử Go API Gateway Service
```bash
cd apps/ml-platform/go-api
go test -v ./...
```

### 3. Kiểm thử Python ML Worker & ONNX Exporter
```bash
cd apps/ml-platform/python-ml-worker
pytest tests/
```

---

## 📂 Cấu Trúc Thư Mục (Directory Structure)

```text
apps/ml-platform/
├── .env                       # Centralized Environment Config File
├── .env.example               # Template environment configuration
├── go-api/                    # Go REST API Gateway Service
│   ├── cmd/main.go
│   ├── internal/ (config, handler, ipc, test)
│   └── go.mod
├── rust-inference/            # High-performance Rust ONNX & UDS IPC Engine
│   ├── src/ (config, error, evidence, inference, ipc, storage)
│   ├── tests/ (onnx_engine_test, uds_ipc_test, evidence_test, model_parity_test)
│   └── Cargo.toml
├── python-ml-worker/          # Python Model Training Engine
│   ├── src/ (config, dataset_loader, trainer, onnx_exporter)
│   └── tests/
└── README.md                  # Hướng dẫn chi tiết hệ thống ML Platform
```
