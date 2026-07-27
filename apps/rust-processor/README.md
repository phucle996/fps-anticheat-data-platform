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

## ⚡ Cơ Chế Hoạt Động Cốt Lõi (Core Operating Mechanism)

1. **Kafka Stream Consumption (Manual Commit At-Least-Once)**:
   - Sử dụng `rdkafka` StreamConsumer tắt tự động commit offset (`enable.auto.commit = false`).
   - Duy trì Offset Tracking Map theo dõi chính xác từng Partition Offset range (`min_offset` -> `max_offset`).
2. **RAM Micro/Macro Batch Accumulation**:
   - Gom nhóm bản ghi dựa trên 3 kích hoạt ngưỡng song song:
     - Ngưỡng số bản ghi (`BATCH_SIZE`, ví dụ 1,000 bản ghi).
     - Ngưỡng dung lượng byte (`MAX_BATCH_BYTES`, ví dụ 5 MB).
     - Ngưỡng thời gian chờ (`FLUSH_INTERVAL_MS`, ví dụ 1,000 ms).
3. **Semantic Data Quality & Dead-Letter Queue (DLQ)**:
   - Kiểm định 11 quy tắc mã hóa ngữ nghĩa (Schema, Null checks, Out-of-bounds metrics).
   - Bản ghi vi phạm tự động phân luồng ghi dạng JSON sang vùng đệm vi phạm MinIO S3 (`bronze/invalid/year=YYYY/...`) để không làm nghẽn luồng xử lý chính.
4. **In-Batch Deduplication & Arrow-Parquet Transformation**:
   - Khử trùng lặp bản ghi trùng `event_id` ngầm định trong Batch bằng SHA-256 Hash Set.
   - Chuyển đổi dữ liệu sang dạng cột **Apache Arrow RecordBatch** 19 trường dữ liệu và nén **Apache Parquet (Zstandard)** đạt tỷ lệ nén 4.2x.
5. **Durable Two-Phase Commit (2PC) & Audit Log**:
   - Phase 1: Upload Parquet file lên MinIO S3 (`bronze/player-stat/...`).
   - Phase 2: Upload Audit Manifest JSON chứa SHA-256 checksum & metadata.
   - Chỉ sau khi Phase 1 & 2 thành công 100%, tiến hành commit Partition Offset lên Kafka Broker.

---

## 🔄 Dynamic Worker Allocation Pool (`src/worker/dynamic_pool.rs`)

Cơ chế điều phối tự động cấp phát và quản lý tải lượng công việc tới các R Worker subprocesses dựa trên dung lượng hàng chờ đệm và số lượng nhiệm vụ cần xử lý:

```text
+-----------------------------------------------------------------------------------------+
|                  DYNAMIC WORKER ALLOCATION POOL DEDICATED FLOW                          |
+-----------------------------------------------------------------------------------------+

                  Batch Manifest Signal / Task Dispatch Requests
                                         |
                                         v
                      +--------------------------------------+
                      |        RDynamicWorkerPool            |
                      |   (Task Dispatcher & Capacity)       |
                      +--------------------------------------+
                                         |
                         +---------------+---------------+
                         |                               |
                         v                               v
             +-----------------------+       +-----------------------+
             | Dynamic Worker        |       | Dynamic Worker        |
             | Thread 1 (Active)     |       | Thread N (Max Capacity|
             +-----------------------+       |  Limit = 64 Workers)  |
                         |                   +-----------------------+
                         v                               |
             +-------------------------------------------------------+
             |           Async Subprocess Execution (R Script)       |
             +-------------------------------------------------------+
```

### 🧠 Giải Thích Chi Tiết Dynamic Worker Allocation:
- **Tự động Co/Giãn theo Tải (Dynamic Capacity Allocation)**:
  - Khi luồng dữ liệu thô nạp vào liên tục tạo các Batch Manifest mới, `RDynamicWorkerPool` cấp phát linh hoạt các Worker Threads để kích hoạt R Worker subprocesses xử lý song song (Tối đa `max_capacity = 64` workers).
- **Thu Hồi Tài Nguyên (Resource Reclamation)**:
  - Ngay khi công việc xử lý batch hoàn tất, subprocess nhàn rỗi được tự động giải phóng để giải phóng CPU core và RAM cho cụm Kubernetes/HA Node.

---

## 🛡️ Resource-Driven Hysteresis Circuit Breaker (`src/worker/circuit_breaker.rs`)

Bộ ngắt mạch bảo vệ an toàn hạ tầng dựa trên việc đo lường trực tiếp tài nguyên Linux OS real-time từ `/proc/stat` & `/proc/meminfo`:

```text
+-----------------------------------------------------------------------------------------+
|             RESOURCE-DRIVEN HYSTERESIS CIRCUIT BREAKER & WATERMARKS                     |
+-----------------------------------------------------------------------------------------+

              Real-Time OS Resource Metrics (/proc/stat & /proc/meminfo)
                                         |
                                         v
                      +--------------------------------------+
                      |      ResourceCircuitBreaker          |
                      +--------------------------------------+
                                  |              |
    (CPU >= 80.0% HOẶC RAM >= 85.0%) |              | (Chỉ khi CPU <= 75.0% VÀ RAM <= 80.0%)
         HIGH WATERMARK TRIPPED   |              |   HYSTERESIS GAP RECOVERED
                                  v              v
                      +------------------+    +------------------+
                      |   State: OPEN    |    |  State: CLOSED   |
                      |  (Tạm ngắt spawn |    |  (Cho phép dispatch|
                      |   R Worker mới)  |    |   task R Worker) |
                      +------------------+    +------------------+
```

### 🧠 Giải Thích Chi Tiết Cơ Chế Hysteresis Gap & Watermarks:
1. **Pure Resource-Driven Monitoring (`/proc/stat` & `/proc/meminfo`)**:
   - Engine đọc trực tiếp chỉ số CPU usage % và RAM usage % từ Linux kernel `procfs` real-time trước khi dispatch bất kỳ công việc nào.
2. **High Watermark (Tripped to `OPEN`)**:
   - Khi **CPU $\ge$ 80.0%** hoặc **RAM $\ge$ 85.0%**, Circuit Breaker ngay lập tức chuyển sang trạng thái `OPEN`.
   - Ngắt việc khởi tạo (spawn) thêm R Workers mới để bảo vệ luồng nạp dữ liệu thô (Ingestion Pipeline) không bị crash do sụp đĩa hoặc đứt kết nối.
3. **Low Watermark & Hysteresis Gap (Recovered to `CLOSED`)**:
   - Để tránh hiện tượng "đóng/mở liên tục" (Flapping) khi tài nguyên dao động quanh ngưỡng trần, hệ thống áp dụng **Hysteresis Gap**: Circuit Breaker chỉ phục hồi về trạng thái `CLOSED` khi **CPU giảm xuống $\le$ 75.0%** VÀ **RAM giảm xuống $\le$ 80.0%**.

---

## 📂 Cấu Trúc Module Dự Án (`src/`)

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
├── worker/                 # High Availability Dynamic Pool & Circuit Breaker
│   ├── mod.rs
│   ├── dynamic_pool.rs     # RDynamicWorkerPool (Pure Resource-Driven Auto-Scaling)
│   ├── circuit_breaker.rs  # ResourceCircuitBreaker (Hysteresis Gap: 80%/85% High, 75%/80% Low)
│   └── r_spawner.rs        # RWorkerSpawner (Async Subprocess Executor)
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
*Hệ thống đã hoàn thành **20+ Unit Tests** phủ 100% chức năng từ Linux `/proc` Hysteresis Circuit Breaker, Kafka Deserialization, Batching, Data Quality Rules, Arrow Transformation, Parquet Zstd Serialization đến Two-Phase Commit Storage.*
