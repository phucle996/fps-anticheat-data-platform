# Unified Machine Learning Platform (`apps/ml-platform`)

The **`apps/ml-platform/`** directory contains a 3-in-1 microservices ecosystem responsible for training Machine Learning models, executing high-speed AI inference, and providing a REST API Gateway serving real-time fraud detection for the **PUBG PC Anti-Cheat Data Platform**.

---

## 🏛️ 3-in-1 Unified Architecture

```text
+-----------------------------------------------------------------------------------------+
|                ML PLATFORM ARCHITECTURE OVERVIEW (MINIO PERSISTENCE)                    |
+-----------------------------------------------------------------------------------------+

   +--------------------------+                         +--------------------------+
   |   Streamlit / Clients    | <--- [ HTTP REST ] ---> |      Go API Gateway      |
   +--------------------------+    /api/v1/predict      +--------------------------+
                                                                     |
                                                                     |  (Unix Domain Socket IPC)
                                                                     |  /tmp/rust_inference.sock
                                                                     |  [ Latency < 0.1ms ]
                                                                     v
   +--------------------------+                         +--------------------------+
   | Shared Local Cache       | <--- [ Load ONNX ] ---- |   Rust Inference Engine  |
   |   (models/v1/model.onnx) |                         +--------------------------+
   +--------------------------+                                 ^        ^
                 ^                                              |        | (Self-Recovery Download
                 | (Export Local)    +--------------------------+        |  when local missing)
                 |                   | (UDS Hot-Swap Signal)             |
   +-----------------------------------------------------------------+   |
   |                 Python ML Training Worker Engine                |   |
   +-----------------------------------------------------------------+   |
                 |                                                       |
                 +---------- [ Upload S3 pubg-models ] ------------------+
                                     │
                                     ▼
                     +-------------------------------+
                     |  MinIO S3 Model Bucket        |
                     |  (s3://pubg-models/v1/...)   |
                     +-------------------------------+
```

---

## 🚀 Core Sub-Services

### 1. `apps/ml-platform/python-ml-worker`
- **Functionality**: Trains Machine Learning models (Random Forest, HistGradientBoosting, Isolation Forest) on Gold Features.
- **ONNX Model Exporter**: Exports models to standard ONNX 6-input format (`kills_pm`, `damage_pm`, `headshot_ratio`, `damage_per_kill`, `movement_pm`, `perf_vs_lobby`).
- **Signal**: Sends an IPC notification signal to the Rust Inference Engine after publishing a new model version.

### 2. `apps/ml-platform/rust-inference`
- **Functionality**: Pure Rust ONNX inference engine using the `ort` crate combined with Atomic Hot-Swap RAM (`Arc<RwLock<LoadedModel>>`) ensuring zero downtime.
- **UDS IPC Server**: Listens on Unix Domain Socket `/tmp/rust_inference.sock`, receives JSON requests, and returns Anomaly Risk Score (0.0 - 1.0) & Risk Level (`LOW`, `MEDIUM`, `HIGH`, `CRITICAL`).
- **Evidence Matrix Generator**: Calculates Robust Z-Score ($Z_{robust} = \frac{X_i - \text{Median}}{\text{MAD} \times 1.4826}$) and extracts top key suspicious cheating indicators.
- **Parquet Storage Writer**: Bundles full inference results into Parquet format and persists to MinIO Data Lake (`s3://pubg-predictions/`).

### 3. `apps/ml-platform/go-api`
- **Functionality**: Secure HTTP REST API Gateway serving Frontend applications and the Streamlit Dashboard.
- **Endpoints**:
  - `GET /api/v1/health`: Checks liveness and UDS IPC socket file connectivity.
  - `POST /api/v1/predict`: Receives prediction payload and forwards to the Rust Engine via UDS IPC.
  - `GET /api/v1/dataset/summary`: Provides 10 data KPI summary statistics for the Streamlit Dashboard.

---

## ⚙️ Unified Environment Configuration (`apps/ml-platform/.env`)

All 3 services within the ML Platform share a common `.env` configuration file adhering to a **100% Fail-Close (Zero Default Fallback)** policy:

```ini
# Go API Gateway Configuration
HTTP_PORT=8081
IPC_SOCKET_PATH=/tmp/rust_inference.sock

# MinIO S3 Object Storage Configuration
MINIO_ENDPOINT=http://localhost:9000
MINIO_BUCKET_DATA=fps-anticheat-datalake
MINIO_BUCKET_MODEL=pubg-models
MINIO_ACCESS_KEY=minioadmin
MINIO_SECRET_KEY=minioadmin

# Kafka Message Broker Configuration
KAFKA_BROKERS=localhost:9092
KAFKA_TOPIC_GOLD=pubg.v1.dataset.gold.ready
KAFKA_TOPIC_MODEL=pubg.v1.ml.model.ready
```

---

## 🛠️ Testing & Execution Commands

### 1. Test Rust Inference Engine, UDS IPC & Evidence Matrix
```bash
cd apps/ml-platform/rust-inference
cargo test -- --nocapture
```

### 2. Test Go API Gateway Service
```bash
cd apps/ml-platform/go-api
go test -v ./...
```

### 3. Test Python ML Worker & ONNX Exporter
```bash
cd apps/ml-platform/python-ml-worker
pytest tests/
```

---

## 📂 Directory Structure

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
└── README.md                  # Detailed ML Platform documentation
```
