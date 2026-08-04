# 🛡️ FPS Anti-Cheat Data Platform

An end-to-end PUBG cheating detection processing and analytics system built using a **Cloud-native-inspired Event-Driven Streaming Data Platform** architecture.

Static data from Kaggle is streamed through Kafka, processed at high throughput using **100% Native Rust (Apache Arrow & Parquet)** into a MinIO Data Lake, trained with **NVIDIA GPU CUDA hardware acceleration** (Python + ONNX), served for real-time inference via a Rust ONNX Engine over Unix Domain Sockets, and visualized on a Streamlit Analytics Dashboard.

---

## 1. 🏗️ Architecture Overview

```text
+==========================================================================================+
|                      FPS ANTI-CHEAT DATA PLATFORM — E2E ARCHITECTURE                     |
+==========================================================================================+

  ┌─────────────────────┐      ────── HTTPS API ──────>     ┌─────────────────────────────┐
  │ Kaggle PUBG Dataset │                                   │    Go Ingestor (Go 1.26)    │
  │ (kill_match_stats   │                                   │ • Normalize & Validate      │
  │  _final_0.csv ~1.9M)│                                   │ • Micro-Batch Ingestion     │
  │                     │                                   │ • Checkpoint Resume (S3)    │
  │                     │                                   │ • Fail-Close & Graceful Stop│
  └─────────────────────┘                                   └──────────────┬──────────────┘
                                                                           │
                                                               JSON Events │ (Key: match_id)
                                                                           v
                                                            ┌─────────────────────────────┐
                                                            │  Apache Kafka (KRaft 7.6.1) │
                                                            │  • raw topic (Partitions: 6)│
                                                            │  • invalid topic (DLQ)      │
                                                            └──────────────┬──────────────┘
                                                                           │ Consumer Group
                                                                           v
  ┌───────────────────────────┐                             ┌─────────────────────────────┐
  │ MinIO S3 Data Lake        │                             │ Native Rust Processor       │
  │ • Bronze (Raw Parquet)    │  <── Parquet + Manifest ──  │ • Consume Kafka -> Validate │
  │ • Silver (Clean Entities) │                             │ • Native Arrow & Parquet    │
  │ • Gold (Feature Matrix)   │                             │ • Dynamic Worker Pool       │
  │ • Models & Predictions    │                             │ • Pure Native Rust Pipeline │
  └─────────────┬─────────────┘                             └──────────────┬──────────────┘
                │                                                          │
                │ Gold Features S3 Read                     Kafka Signal:  │ gold.ready
                v                                                          v
  ┌────────────────────────────────────────────────────────────────────────────────────────┐
  │ ML Platform 3-in-1 Ecosystem (apps/ml-platform)                                        │
  │                                                                                        │
  │ ┌───────────────────────────────┐  ──── Export ONNX ────>  ┌─────────────────────────┐ │
  │ │ Python ML Worker (Python 3.13)│                            │ Rust Inference Engine   │ │
  │ │ • Supervised XGBoost CUDA GPU │  Save to s3://pubg-models/ │ • ONNX Runtime CUDA     │ │
  │ │ • Export 5-feature ONNX Model │  Signal: model.ready      │ • Pure GPU Acceleration │ │
  │ └───────────────────────────────┘                            └────────────┬────────────┘ │
  │                                                                           │             │
  │                                                             Unix Socket   │ IPC Req/Resp│
  │                                                             /tmp/*.sock   │ (Low Latency│
  │                                                                           v             │
  │                                                            ┌──────────────────────────┐ │
  │                                                            │ Go REST API Gateway      │ │
  │                                                            │ • GET /api/v1/health/sum │ │
  │                                                            │ • POST /api/v1/predict   │ │
  │                                                            └──────────────┬───────────┘ │
  └─────────────┬─────────────────────────────────────────────────────────────┼─────────────┘
                │                                                             │
                │ S3 Read (Silver / Gold / Predictions)                       │ HTTP REST Client
                v                                                             v
  ┌────────────────────────────────────────────────────────────────────────────────────────┐
  │ Streamlit Risk Analysis & Telemetry Dashboard (Python 3.13 / Streamlit, Port 8501)    │
  │ Views: Overview (KPIs) | Data Quality | Player Analysis | Risk Fraud & Model Registry  │
  └────────────────────────────────────────────────────────────────────────────────────────┘
```

---

## 2. 📂 Monorepo Structure

```text
fps-anticheat/
├── apps/                             # Microservices & Data Processors
│   ├── go-ingestor/                  # [Go 1.26] Kaggle Dataset Downloader & Kafka Stream Replay Engine
│   ├── rust-structured-streamer/     # [Rust 1.88] Native Structured Streamer (Apache Arrow/Parquet S3)
│   ├── ml-platform/                  # 3-in-1 Ecosystem (NVIDIA GPU Accelerated):
│   │   ├── python-ml-worker/         #   [Python 3.13] XGBoost (CUDA GPU) Training & ONNX Exporter
│   │   ├── rust-inference/           #   [Rust 1.88] Sub-millisecond ONNX Inference Engine (CUDA) + UDS IPC
│   │   └── go-api/                   #   [Go 1.26] REST API Gateway
│   └── streamlit-dashboard/          # [Python 3.13 / Streamlit] Analytics & Risk Operator Dashboard
├── contracts/                        # Shared Data Contracts (JSON Schemas)
│   ├── schemas/                      # Event Envelope, Manifest & Prediction Schemas
│   ├── examples/                     # Sample valid/invalid telemetry events
│   └── data-dictionary/              # Metric definitions dictionary
├── configs/                          # Environment configuration files (local, dev, production)
├── init/                             # Provisioning scripts for Kafka topics & MinIO buckets
├── docs/                             # Architecture Specs & Test Suite documentation
│   ├── DATA_LAKE_LAYOUT.md           # Medallion Data Lake specifications
│   ├── DOCKER_INTEGRATION_TEST_SUITE.md
│   └── EDA_REPORT.md
├── scripts/                          # Setup & testing shell utilities
├── docker-compose.yml                # Main Docker Compose manifest (NVIDIA GPU Passthrough Enabled)
├── Makefile                          # Unified multi-language monorepo CLI commands
├── ARCH.md                           # Deep-dive architecture design document
└── checklist.md                      # Project roadmap & implementation checklist
```

---

## 3. 📡 Kafka Topic Registry

| Topic | Partitions | Retention | Producer | Consumer | Description |
|:---|:---:|:---:|:---|:---|:---|
| `pubg.v1.player-stat.raw` | 6 | 24h | Go Ingestor | Rust Processor | Valid Event Envelopes (`match_summary` + `kill_event`), key = `match_id` |
| `pubg.v1.kill-event.raw` | 6 | 24h | Go Ingestor | Rust Processor | Dedicated kill telemetry events (`match_deaths` schema) |
| `pubg.v1.invalid` | 3 | 30 days | Go Ingestor | (Audit) | Dead-Letter Queue (DLQ) for validation failures |
| `pubg.v1.dataset.gold.ready` | 1 | 24h | Native Rust Processor | Python ML Worker | Notification signal when a Gold Parquet batch is ready |
| `pubg.v1.ml.model.ready` | 1 | 7 days | Python ML Worker | Rust Inference Engine | Notification signal when a new ONNX model version is published |

---

## 4. 💾 MinIO Data Lake Layout (Medallion)

```text
fps-anticheat-datalake/
├── bronze/                           # Raw Ingestion Layer
│   ├── player-stat/                  # Parquet files from Rust Processor
│   └── invalid/                      # Data Quality error JSON records
├── manifests/                        # Audit log JSON manifests
├── silver/                           # Cleaned & Modeled Entities
│   ├── players/                      # Player profiles
│   ├── matches/                      # Match summaries
│   └── player-match/                 # Detailed Player-Match statistics
├── gold/                             # Feature Store for ML Training
│   └── player-match-features/        # Feature Matrix (Parquet)
├── models/                           # ML Model Artifacts (ONNX, metadata, policies)
└── predictions/                      # Anomaly Detection scoring results
```

---

## 5. 🔗 Service Directory & Documentation

| # | Service | Tech Stack | Documentation |
|:---:|:---|:---|:---|
| 1 | **Go Ingestor** | Go 1.26, Kafka, MinIO | [apps/go-ingestor/README.md](apps/go-ingestor/README.md) |
| 2 | **Native Rust Structured Streamer** | Rust 1.88, Apache Arrow 52, Parquet | [apps/rust-structured-streamer/README.md](apps/rust-structured-streamer/README.md) |
| 3 | **ML Platform (3-in-1)** | Python 3.13 (XGBoost CUDA), Rust 1.88 (ONNX CUDA), Go 1.26 | [apps/ml-platform/README.md](apps/ml-platform/README.md) |
| 4 | **Streamlit Dashboard** | Python 3.13, Streamlit | *(see `apps/streamlit-dashboard/app.py`)* |

---

## 6. 🚀 Quick Start

### Prerequisites
- **Go** ≥ 1.26
- **Rust (Cargo)** ≥ 1.88
- **Python** ≥ 3.13
- **NVIDIA GPU & Drivers** (`nvidia-smi` active and working)
- **NVIDIA Container Toolkit** (`nvidia-ctk` configured for Docker runtime)
- **Docker & Docker Compose** (BuildKit enabled)

### Infrastructure Initialization

```bash
# Initialize everything (.env, S3 Buckets, Kafka Topics, Kaggle Sync & Services)
make init

# Or launch Docker Compose directly
docker compose up -d
```

### Useful Makefile Commands

```bash
make help         # List all available CLI commands
make check-deps   # Verify Go, Rust, Python3, and Docker environments
make init         # Provision .env, launch containers, create S3 Buckets / Kafka Topics & fetch Kaggle Dataset
make start        # Start all infrastructure containers
make run          # Start real-time Replay Stream (Auto-Resumes from S3 Checkpoint)
make run-reset    # Reset S3 Checkpoint and replay Kaggle CSV dataset from row 1
make stop         # Pause all running containers (retains data volumes)
make restart      # Restart all containers
make purge        # Complete cleanup of containers, Docker volumes & temporary files (Zero-State)
make test         # Run test suites across all languages (Go, Rust, Python)
make fmt          # Format code across the entire monorepo (Go, Rust)
make logs         # Tail real-time logs from all containers
```

---

## 📋 Project Roadmap & Progress

Refer to [checklist.md](checklist.md) for detailed task tracking and progress milestones.
