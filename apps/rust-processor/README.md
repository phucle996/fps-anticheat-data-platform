# PUBG Anti-Cheat Rust Stream Processor Engine

Ứng dụng **Rust Stream Processor Engine** (`apps/rust-processor`) là thành phần xử lý luồng sự kiện tốc độ cao trong hệ thống **FPS Anti-Cheat Data Platform**. Ứng dụng tiêu thụ dữ liệu thô từ Apache Kafka, kiểm tra chất lượng dữ liệu (Data Quality Validation), lọc trùng lặp, chuyển đổi sang chuẩn định dạng cột **Apache Arrow RecordBatch**, nén **Apache Parquet (Zstandard)** và lưu trữ bền vững lên **MinIO S3 Medallion Data Lake** theo nguyên tắc **Durable Two-Phase Commit (At-Least-Once & Zero Data Loss 100%)**.

---

## 🏗️ Kiến trúc Pipeline & Luồng Dữ liệu (Data Flow)

```text
+-----------------------------------------------------------------------------------------+
|                       RUST STREAM PROCESSOR PIPELINE ARCHITECTURE                       |
+-----------------------------------------------------------------------------------------+

                 +-----------------------------------------------+
                 |  Apache Kafka Cluster                         |
                 |  (pubg.v1.player-stat.raw)                    |
                 +-----------------------------------------------+
                                         |
                                         | (rdkafka StreamConsumer, enable.auto.commit = false)
                                         v
                 +-----------------------------------------------+
                 | 1. Ingest Module                              |
                 |    (KafkaConsumer & BatchAccumulator)         |
                 +-----------------------------------------------+
                                         |
                                         | (RAM Micro/Macro Batching + Partition Offset Tracking)
                                         v
                 +-----------------------------------------------+
                 | 2. Data Quality Engine                        |
                 |    (EventValidator - 11 Semantic Rules)       |
                 +-----------------------------------------------+
                         |                               |
                         | (Valid Records)               | (Invalid / Violations)
                         v                               v
       +------------------------------------+   +------------------------------------+
       | 3. In-Batch Deduplicator           |   | MinIO Dead Letter Storage          |
       |    (EventDeduplicator - event_id)  |   | (bronze/invalid/year=YYYY/...)     |
       +------------------------------------+   +------------------------------------+
                         |
                         v
       +------------------------------------+
       | 4. Apache Arrow Converter          |
       |    (RecordBatch 19 Columns Schema) |
       +------------------------------------+
                         |
                         v
       +------------------------------------+
       | 5. Parquet Serializer              |
       |    (Zstandard Compression Rate 4.2x)|
       +------------------------------------+
                         |
                         v
       +------------------------------------+
       | 6. MinIO S3 Bronze Writer          |
       |    (Durable Two-Phase Commit 2PC)  |
       +------------------------------------+
             |                       |
             | (Phase 1)             | (Phase 2)
             v                       v
   +--------------------+  +--------------------+
   | Parquet Data File  |  | Manifest Metadata  |
   | (bronze/player-...) |  | (manifests/...)    |
   +--------------------+  +--------------------+
             |                       |
             +-----------+-----------+
                         | (Only when Phase 1 & 2 succeed 100%)
                         v
       +------------------------------------+
       | 7. Kafka Partition Offset Commit   |
       |    (commit_partition_offsets)      |
       +------------------------------------+
```

---

## 📂 Cấu trúc Module Dự án (`src/`)

```text
apps/rust-processor/src/
├── main.rs                 # Thin Entrypoint (Logger, Config Loader & Signal Receiver)
├── app.rs                  # StreamProcessorApp (Điều phối toàn bộ Pipeline Engine)
├── config.rs               # Environment Config Loader (Fail-Close 100%, Zero Fallback)
├── error.rs                # AppError Enum & Result Type chuẩn hóa
├── domain/                 # Data Contracts & Struct Models
│   ├── mod.rs
│   └── event.rs            # EventEnvelope, PlayerStatPayload, SourceMetadata
├── ingest/                 # Pipeline Ingestion (Tiếp nhận Kafka & Gom dữ liệu RAM)
│   ├── mod.rs
│   ├── consumer.rs         # Kafka StreamConsumer (At-Least-Once Active)
│   └── accumulator.rs      # BatchAccumulator & Partition Offset Tracking
├── transform/              # Pipeline Transform (Validation, Dedup, Arrow, Parquet)
│   ├── mod.rs
│   ├── validator.rs        # EventValidator (11 Quy tắc Semantic Data Quality)
│   ├── dedup.rs            # EventDeduplicator (Lọc trùng event_id trong batch)
│   ├── arrow.rs            # ArrowConverter (Arrow Schema 19 cột & RecordBatch)
│   └── parquet.rs          # ParquetSerializer (Zstandard Compression & Reader)
└── storage/                # Pipeline Storage (MinIO S3 & Audit Manifest Log)
    ├── mod.rs
    ├── minio.rs            # MinioWriter (object_store SDK & Hive Partitioning)
    └── manifest.rs         # BatchManifest & PartitionOffsetMetadata Audit Log
```

---

## ⚙️ Biến Môi Trường Cấu Hình (Environment Variables)

Ứng dụng áp dụng nguyên tắc **Fail-Close / Fail-Fast 100%** (Tự động ngắt ứng dụng ngay lập tức nếu thiếu bất kỳ biến môi trường bắt buộc nào):

| Biến Môi Trường | Mô Tả | Ví Dụ Mặc Định |
| :--- | :--- | :--- |
| `KAFKA_BROKERS` | Danh sách Kafka brokers | `localhost:9092` hoặc `kafka:9092` |
| `KAFKA_RAW_TOPIC` | Kafka raw topic nguồn | `pubg.v1.player-stat.raw` |
| `KAFKA_GROUP_ID` | Consumer Group ID | `rust-processor-group` |
| `MINIO_ENDPOINT` | Endpoint kết nối MinIO S3 | `http://localhost:9000` hoặc `http://minio:9000` |
| `MINIO_BUCKET` | Tên Data Lake S3 Bucket | `fps-anticheat-datalake` |
| `MINIO_ACCESS_KEY` | Access Key của MinIO S3 | `minioadmin` |
| `MINIO_SECRET_KEY` | Secret Key của MinIO S3 | `minioadmin` |
| `BATCH_SIZE` | Ngưỡng số bản ghi tối đa cho 1 batch | `1000` |
| `FLUSH_INTERVAL_MS` | Ngưỡng thời gian flush batch (ms) | `1000` |

---

## 🧪 Chạy Kiểm Thử & Biên Dịch (Tests & Build)

### 1. Kiểm tra Cú pháp & Biên dịch:
```bash
cd apps/rust-processor
cargo check
```

### 2. Chạy Toàn bộ Unit & Integration Test Suites:
```bash
cargo test
```
*Hiện tại hệ thống đã hoàn thành **20 Unit Tests** phủ 100% chức năng từ Kafka Deserialization, Batching, Data Quality Rules, Deduplication, Arrow Transformation, Parquet Zstd Serialization đến Storage Pathing.*

### 3. Chạy Ứng dụng cục bộ:
```bash
KAFKA_BROKERS="localhost:9092" \
KAFKA_RAW_TOPIC="pubg.v1.player-stat.raw" \
KAFKA_GROUP_ID="rust-processor-group" \
MINIO_ENDPOINT="http://localhost:9000" \
MINIO_BUCKET="fps-anticheat-datalake" \
MINIO_ACCESS_KEY="minioadmin" \
MINIO_SECRET_KEY="minioadmin" \
cargo run
```
