# Go Dataset Ingestor Service (`apps/go-ingestor`)

The **Go Dataset Ingestor** service acts as the central Raw Data Ingestor for the entire **PUBG PC Anti-Cheat Data Platform**.

The application downloads/ingests PUBG PC Telemetry datasets from Kaggle/CSV files, normalizes raw telemetry into the **Canonical Event Envelope Schema**, performs base data quality validation, and streams the event pipeline into the **Kafka Raw Topic (`pubg.v1.player-stat.raw`)**.

---

## 🏛️ System Architecture

The service is designed using a **Clean Modular Pipeline Architecture** with strict Separation of Concerns:

```text
+-----------------------------------------------------------------------------------------+
|                    GO DATASET INGESTOR MODULAR ARCHITECTURE                             |
+-----------------------------------------------------------------------------------------+

  [ CLI Entrypoints ]        cmd/dataset-sync/main.go        cmd/replay/main.go
                                         |                           |
                                         v                           v
  [ Application Layer ]      +---------------------------------------------------+
                             |               internal/app                        |
                             |      (DatasetSyncService, ReplayService)          |
                             +---------------------------------------------------+
                                   /             |                \
                                  v              v                 v
  [ Processing Pipeline ]  internal/pipeline     |            internal/domain
                           (parser, normalizer,  |            (contract envelopes,
                            batcher, worker_pool)|             invalid records)
                                                 |
  [ External Infrastructure ]                    v
                          +------------------------------------------------------+
                          | internal/storage  | internal/kafka | internal/provider|
                          | (MinIO S3 SDK,    | (Segmentio     | (Kaggle REST API |
                          |  Checkpoint Engine|  Producer)     |  Client)         |
                          +------------------------------------------------------+
```

---

## 🔄 Data Flow

The end-to-end processing data pipeline from raw source to the Kafka Brokers follows 5 main stages:

```text
+-----------------------------------------------------------------------------------------+
|                           DATA FLOW PIPELINE STAGES                                     |
+-----------------------------------------------------------------------------------------+

                 +-----------------------------------------------+
                 |  Kaggle PUBG Telemetry / Local CSV File       |
                 |  (pubg-match-ground-truth.csv)                |
                 +-----------------------------------------------+
                                         |
                                         | 1. Stream Reading (provider.KaggleClient / pipeline.CSVParser)
                                         v
                 +-----------------------------------------------+
                 | 1. Kaggle Downloader / CSV Reader             |
                 |    (Zero-RAM Streaming CSV Buffer Reader)     |
                 +-----------------------------------------------+
                                         |
                                         | 2. Anonymization & Normalization
                                         v
                 +-----------------------------------------------+
                 | 2. Canonical Schema Normalizer                |
                 |    (pipeline.PlayerStatNormalizer - SHA-256)  |
                 +-----------------------------------------------+
                                         |
                                         | 3. Quality Rules Verification
                                         v
                 +-----------------------------------------------+
                 | 3. Base Data Quality Validator                |
                 |    (Pass -> EventEnvelope | Fail -> DLQ Record)|
                 +-----------------------------------------------+
                                         |
                                         | 4. Micro-Batch Accumulation & Checkpointing
                                         v
                 +-----------------------------------------------+
                 | 4. Batching & Checkpoint State Engine         |
                 |    (pipeline.BatchFlusher + storage.Checkpoint|
                 +-----------------------------------------------+
                                         |
                                         | 5. High-Throughput Async Produce (kafka.KafkaProducer)
                                         v
                 +-----------------------------------------------+
                 |  Apache Kafka Cluster                         |
                 |  (Topic: pubg.v1.player-stat.raw / DLQ)       |
                 +-----------------------------------------------+
```

---

## ⚙️ Environment Configuration (Fail-Close Enforced)

| Environment Variable | Description | Example |
| :--- | :--- | :--- |
| `KAFKA_BROKERS` | List of Kafka Brokers | `localhost:9092` |
| `KAFKA_TOPIC_RAW` | Target Kafka topic for raw event stream | `pubg.v1.player-stat.raw` |
| `KAFKA_INVALID_TOPIC` | Target Kafka topic for Dead-Letter Queue (DLQ) | `pubg.v1.invalid` |
| `KAGGLE_USERNAME` | Kaggle API Username | `my_kaggle_user` |
| `KAGGLE_KEY` | Kaggle API Token Key | `1234567890abcdef` |
| `MINIO_ENDPOINT` | MinIO S3 Object Storage Endpoint | `localhost:9000` |
| `MINIO_BUCKET` | Main Bucket name for Data Lake storage | `fps-anticheat-datalake` |
| `MINIO_ACCESS_KEY` | MinIO Access Key | `minioadmin` |
| `MINIO_SECRET_KEY` | MinIO Secret Key | `minioadmin` |

---

## 🛠️ Execution Commands

### 1. Run Unit Test Suite
```bash
cd apps/go-ingestor
go test -v ./...
```

### 2. Launch Dataset Synchronization from Kaggle
```bash
cd apps/go-ingestor
go run cmd/dataset-sync/main.go
```

### 3. Launch Streaming Replay Daemon to Kafka
```bash
cd apps/go-ingestor
go run cmd/replay/main.go
```

---

## 📂 Directory Structure

```text
apps/go-ingestor/
├── cmd/
│   ├── dataset-sync/main.go   # CLI Entrypoint for Kaggle dataset synchronization
│   └── replay/main.go         # CLI Entrypoint for Kafka Replay Streaming Daemon
├── internal/
│   ├── app/                   # Application Orchestration & Use Cases (dataset_sync, replay)
│   ├── config/                # Fail-Close Environment Configuration Loader
│   ├── contract/              # Business Event Envelopes & DLQ Data Models
│   ├── kafka/                 # Kafka Producer Infrastructure (HA & Fail-Close)
│   ├── logging/               # Logrus JSON Logger Formatter
│   ├── pipeline/              # Pure Data Processing Pipeline (parser, normalizer, batcher, worker_pool)
│   ├── provider/              # External Data Source Providers (Kaggle API client)
│   ├── storage/               # Object Storage & Checkpoint State Engine (minio, checkpoint)
│   └── test/                  # Unit Test Suites
├── go.mod                     # Go module definition
└── README.md                  # Service documentation
```
