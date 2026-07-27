# PUBG Anti-Cheat Data Platform Architecture

## 1. Tổng quan & Nguyên tắc Kiến trúc

PUBG Anti-Cheat Data Platform là một hệ thống xử lý dữ liệu end-to-end mô phỏng và phân tích hành vi bất thường của người chơi PUBG dựa trên dataset công khai từ Kaggle. Dữ liệu tĩnh từ Kaggle được stream qua Kafka, xử lý batch với hiệu năng cao bằng Rust để lưu giữ vào Data Lake (MinIO), sau đó được phân tích, huấn luyện mô hình Machine Learning (Anomaly Detection) bằng R và hiển thị kết quả trực quan trên Shiny Dashboard.

### Đầu ra của hệ thống phân tích:
* `risk_score`: Điểm bất thường (0.0 đến 1.0) của người chơi trong từng trận đấu.
* `risk_level`: Mức độ rủi ro (`LOW`, `MEDIUM`, `HIGH`, `CRITICAL`) được ánh xạ từ `risk_score`.
* `evidence`: Các đặc trưng (features) khiến người chơi bị đánh giá là bất thường.
* `model_version` & `feature_version`: Phiên bản mô hình và bộ đặc trưng sử dụng để đưa ra dự đoán.

### Nguyên tắc Kiến trúc Cloud-Native & HA (High Availability):
1. **Stateless Services & Scalability**: Các thành phần xử lý (Go Ingestor, Rust Batch Processor, R Pipeline) được thiết kế theo dạng stateless, cho phép dễ dàng scale-out chiều ngang trên Kubernetes/Docker Swarm.
2. **Loosely Coupled via Event-Driven & Data Lake**: Kafka đóng vai trò message buffer chịu lỗi (fault-tolerant), phân tách luồng Ingest và Processing. MinIO (S3-compatible Object Storage) làm bộ lưu trữ dữ liệu tập trung bền vững.
3. **Data Integrity & At-Least-Once Delivery**: Đảm bảo không mất dữ liệu bằng cơ chế Kafka Offset Commit chỉ sau khi Rust ghi nhận dữ liệu thành công xuống MinIO.
4. **Resiliency & Race Condition Prevention**: Quản lý ghi dữ liệu theo batch key độc lập (`partition/offset-range`), ngăn ngừa xung đột ghi đồng thời (race condition) và đảm bảo tính nhất quán (idempotency) nhờ khử trùng lặp qua `event_id`.

---

## 2. Topology Kiến trúc Tổng thể

```mermaid
flowchart TD
    subgraph Ingestion ["1. Data Ingestion Layer (Go)"]
        Kaggle["Kaggle PUBG Dataset (CSV)"] -->|HTTPS Download| Sync["cmd/dataset-sync"]
        Sync -->|Local Manifest & Archive| Storage["Local Storage / Volume"]
        Storage -->|Read CSV Stream| Replay["cmd/replay"]
    end

    subgraph Messaging ["2. Event Streaming Layer (Kafka)"]
        Replay -->|JSON Events (Key: match_id)| RawTopic["Topic: pubg.v1.player-stat.raw"]
        Replay -.->|Invalid Rows| DLQTopic["Topic: pubg.v1.invalid"]
    end

    subgraph BatchProcessing ["3. Batch Processing Layer (Rust)"]
        RawTopic -->|Consumer Group| Consumer["Kafka Consumer"]
        Consumer --> Accumulator["Batch Accumulator"]
        Accumulator --> Validation["Schema & Semantic Validation"]
        Validation --> Dedup["Deduplication (event_id)"]
        Dedup --> Arrow["Arrow RecordBatch"]
        Arrow --> Parquet["Parquet Writer (Zstd)"]
    end

    subgraph DataLake ["4. Data Lake Storage (MinIO S3)"]
        Parquet -->|S3 Upload| Bronze["Bronze Zone (Raw Parquet + Manifest)"]
    end

    subgraph AnalyticsML ["5. Analytics & ML Layer (R)"]
        Bronze -->|Read Batch Manifest| Preprocess["R Preprocessing"]
        Preprocess --> Silver["Silver Zone (Cleaned Entities)"]
        Silver --> FeatureEng["R Feature Engineering"]
        FeatureEng --> Gold["Gold Zone (Feature Store)"]
        Gold --> ModelTrain["R Model Training (Isolation Forest)"]
        ModelTrain --> ModelStore["Models Bucket"]
        Gold & ModelStore --> ModelScore["R Model Scoring & Evidence Gen"]
        ModelScore --> Predictions["Predictions Zone"]
    end

    subgraph Visualization ["6. Visualization Layer (R Shiny)"]
        Silver & Gold & Predictions -->|S3 Read| Shiny["Shiny Dashboard"]
    end

    Parquet -.->|Success Upload| Commit["Commit Kafka Offset"]
    Commit -.-> Consumer
```

### Sơ đồ Luồng Dữ liệu (Data Pipeline Flow):
```text
Kaggle CSV ──> Go Sync ──> Local CSV ──> Go Replay ──> Kafka (pubg.v1.player-stat.raw)
                                                               │
Shiny Dashboard <── Predictions <── R Scoring <── MinIO Gold <── Rust Processor (Parquet)
```

---

## 3. Cấu trúc Repository & Ranh giới Service

```text
pubg-anti-cheat-platform/
├── apps/                        # Mã nguồn ứng dụng (Service độc lập)
│   ├── go-ingestor/             # Service tải & replay dữ liệu từ Kaggle vào Kafka
│   ├── rust-processor/          # Service tiêu thụ Kafka, gom batch & ghi Parquet xuống MinIO
│   ├── r-pipeline/              # Service ETL, Feature Engineering & ML Anomaly Detection
│   └── shiny-dashboard/         # Web Dashboard hiển thị phân tích & kết quả rủi ro
├── contracts/                   # Shared Data Contracts (JSON Schemas, Data Dictionaries)
│   ├── schemas/                 # Schema định nghĩa Event Envelope, Manifests, Predictions
│   ├── examples/                # File mẫu event hợp lệ / không hợp lệ
│   └── data-dictionary/         # Từ điển dữ liệu giải thích các trường thông tin
├── configs/                     # File cấu hình môi trường (local, dev, production)
├── data/                        # Thư mục lưu tạm archives, extracted CSV, manifests cục bộ
├── deployments/                 # Orchestration configs (Docker Compose, K8s manifests)
│   └── compose/                 # docker-compose.yml & scripts khởi tạo (Kafka/MinIO)
├── scripts/                     # Shell scripts hỗ trợ setup Kafka, MinIO, demo pipeline
├── docs/                        # Tài liệu kiến trúc (ARCH.md, ADR, Data specs)
└── tests/                       # Integration & End-to-End test suites
```

---

## 4. Chi tiết các Sub-systems (Apps)

### 4.1. Go Dataset Ingestor (`apps/go-ingestor`)
Go Ingestor đảm nhận việc thu thập dữ liệu tĩnh từ Kaggle và mô phỏng thành luồng sự kiện (event stream) thời gian thực.

```text
apps/go-ingestor/
├── cmd/
│   ├── dataset-sync/main.go     # Entry point tải dataset từ Kaggle (One-shot)
│   └── replay/main.go           # Entry point replay CSV thành event stream vào Kafka
└── internal/
    ├── app/                     # Orchestrator cho usecase sync & replay
    ├── config/                  # Quản lý & validate cấu hình YAML / Env
    ├── source/                  # Interface đọc dataset (kaggle source, local source)
    ├── dataset/                 # Quản lý archive, giải nén, checksum SHA-256, manifest
    ├── parser/csv/              # Đọc CSV theo cơ chế Streaming (tiết kiệm RAM)
    ├── normalize/               # Chuẩn hóa kiểu dữ liệu, loại bỏ giá trị rác
    ├── contract/                # Định nghĩa Event Envelope tiêu chuẩn
    ├── replay/                  # Buffer, Flush controller, Rate limiter, Statistics
    └── broker/kafka/            # Kafka Async Producer (partition key = match_id)
```

#### Tiến trình hoạt động:
1. **`dataset-sync`**: Đọc credentials -> Tải zip dataset từ Kaggle -> Kiểm tra SHA-256 checksum -> Giải nén -> Tạo `dataset-manifest.json`.
2. **`replay`**: Đọc manifest -> Stream từng dòng CSV -> Chuẩn hóa & đóng gói thành Event Envelope -> Đưa vào Buffer -> Đẩy vào Kafka theo rate limit quy định.

---

### 4.2. Kafka Topology & Messaging Strategy

Kafka đóng vai trò đệm dữ liệu (decoupling layer), giúp cách ly tốc độ phát event của Go Ingestor với tốc độ xử lý batch của Rust Processor.

```text
                       +-----------------------------+
                       |      Go Dataset Replay      |
                       +--------------+--------------+
                                      |
                         match_id key | JSON Envelope
                                      v
                 +--------------------+--------------------+
                 |                                         |
                 v (Hợp lệ)                                v (Lỗi schema / parse)
+------------------------------------+    +--------------------------------+
| pubg.v1.player-stat.raw            |    | pubg.v1.invalid                |
| Partition Key: match_id            |    | Dead-letter queue / Dead event |
+------------------+-----------------+    +--------------------------------+
                   |
                   | Kafka Consumer Group (Rust Workers)
                   v
+------------------------------------+
| Rust Batch Processor               |
+------------------------------------+
```

* **Topic `pubg.v1.player-stat.raw`**: Chứa toàn bộ event hợp lệ đã chuẩn hóa. Message Key sử dụng `match_id` để đảm bảo toàn bộ dữ liệu của cùng một trận đấu rơi vào cùng một Partition (thuận tiện cho việc phân tích theo match).
* **Topic `pubg.v1.invalid`**: DLQ chứa các bản ghi lỗi cấu trúc hoặc không thể parse trong quá trình replay.

---

### 4.3. Rust Batch Processor (`apps/rust-processor`)
Rust Batch Processor là service hiệu năng cao chịu trách nhiệm đọc event từ Kafka, kiểm tra chất lượng, gom batch và chuyển đổi sang định dạng cột Parquet tối ưu trước khi ghi xuống Data Lake.

```text
apps/rust-processor/
└── src/
    ├── main.rs                  # Entry point dịch vụ Rust
    ├── consumer/kafka.rs        # Kafka Consumer subscribe topic raw
    ├── batch/accumulator.rs     # Gom event theo dung lượng, số lượng bản ghi hoặc timeout
    ├── validation/              # Kiểm tra Schema (JSON schema) và Semantic (logic game)
    ├── dedup/event_id.rs        # Khử trùng lặp bản ghi trong batch dựa trên event_id hash
    ├── transform/player_stat.rs # Biến đổi JSON Event thành bảng dữ liệu phẳng (Flattening)
    ├── arrow/record_batch.rs    # Chuyển đổi dữ liệu sang Apache Arrow RecordBatch in-memory
    ├── parquet/writer.rs        # Chuẩn hóa & nén Parquet bằng Zstandard (zstd)
    ├── storage/minio.rs         # Upload file Parquet & Batch Manifest lên MinIO (S3 API)
    └── manifest/builder.rs      # Khởi tạo Batch Manifest liên kết file Parquet với Offset
```

#### Quy trình xử lý Batch & Cam kết Dữ liệu (Offset Commit Atomicity):
```text
Read Kafka Messages ──> Batch Accumulator ──> Validate & Dedup ──> Arrow RecordBatch
                                                                         │
Kafka Offset Committed <── Manifest Uploaded <── Parquet Uploaded <──────┘
```
Offset của Kafka **chỉ được commit** khi cả file Parquet và file `batch-manifest.json` tương ứng đã được upload thành công lên MinIO. Cơ chế này đảm bảo ngữ nghĩa xử lý **At-Least-Once**, không mất mát dữ liệu khi worker xảy ra sự cố.

---

### 4.4. MinIO Data Lake Architecture (Medallion Pattern)

MinIO đóng vai trò lưu trữ bền vững trung tâm (Data Lake). Dữ liệu được tổ chức theo kiến trúc Medallion 3 lớp (Bronze -> Silver -> Gold):

```text
pubg-data/
├── bronze/                              # Raw Data Zone (Parquet nguyên bản + Metadata)
│   ├── player-stat/partition=0/offset=0-4999/batch.parquet
│   └── invalid/                         # Event không qua được bước validate
├── manifests/                           # Chứa batch-manifest.json tra cứu theo offset
│   └── batch-20260728-000001.json
├── silver/                              # Cleaned & Modeled Entities Zone
│   ├── players/                         # Bảng thông tin định danh player
│   ├── matches/                         # Bảng thông tin trận đấu
│   └── player-match/                    # Thống kê chi tiết từng player theo trận đấu
├── gold/                                # Analytics & Feature Store Zone
│   └── player-match-features/v1/        # Feature dataset đã tinh chế sẵn sàng cho ML
├── models/                              # Lưu trữ phiên bản Model Artifacts (.rds, config)
│   └── pubg-anomaly-detector/v1.0.0/
└── predictions/                         # Kết quả dự đoán Anomaly Score & Evidence
    └── model_version=v1.0.0/score_date=2026-07-28/predictions.parquet
```

---

### 4.5. R Analytics & Machine Learning Pipeline (`apps/r-pipeline`)

R Pipeline chịu trách nhiệm biến đổi dữ liệu từ Bronze thành các tầng phân tích sâu hơn, tính toán bộ đặc trưng và thực thi mô hình phát hiện bất thường (Anomaly Detection).

```text
apps/r-pipeline/
├── R/
│   ├── storage.R                # Wrapper đọc/ghi Parquet & JSON trực tiếp từ MinIO S3
│   ├── manifests.R              # Đọc Batch Manifest để xác định các batch Bronze mới
│   ├── preprocessing.R          # Xử lý missing values, chuẩn hóa kiểu dữ liệu (Bronze -> Silver)
│   ├── features.R               # Trích xuất đặc trưng thống kê & gian lận (Silver -> Gold)
│   ├── training.R               # Huấn luyện mô hình Isolation Forest cho Anomaly Detection
│   ├── scoring.R                # Tính toán Anomaly Score, Risk Score (0-1) & Risk Level
│   └── evidence.R               # Trích xuất bằng chứng bất thường (Robust Z-Score deviation)
└── scripts/                     # Scripts gọi thực thi theo quy trình CLI/Cronjob
```

#### Pipeline Chi Tiết:
1. **Preprocessing**: Đọc manifest Bronze -> Đọc file Parquet -> Làm sạch dữ liệu -> Ghi vào Silver `player-match`.
2. **Feature Engineering**: Đọc Silver -> Tính toán các chỉ số nâng cao:
   * `kills_per_minute`, `damage_per_minute`, `headshot_ratio`
   * `total_distance`, `damage_per_kill`, `performance_vs_lobby`
   -> Ghi vào Gold Zone.
3. **Model Training**: Sử dụng mô hình **Isolation Forest** đọc Gold features -> Huấn luyện phát hiện Outliers -> Lưu Model Artifact (`model.rds`) và metadata vào MinIO bucket `models/`.
4. **Model Scoring & Evidence Generation**: Đọc dữ liệu Gold mới + Model Artifact -> Tính `risk_score` -> Đánh giá `risk_level` -> So sánh chỉ số player với Median của toàn bộ Lobby/Population để tạo bản giải thích `evidence` JSON.

---

### 4.6. R Shiny Dashboard (`apps/shiny-dashboard`)
Shiny Dashboard cung cấp giao diện trực quan cho đội ngũ vận hành và phân tích an ninh game quan sát trạng thái hệ thống và hành vi người chơi.

```text
apps/shiny-dashboard/
├── app.R                        # Main entry point khởi chạy ứng dụng Web Shiny
├── R/
│   ├── data_loader.R            # Tải dữ liệu bất đồng bộ từ MinIO (Silver, Gold, Predictions)
│   ├── overview.R               # Module hiển thị tổng quan hệ thống (Total players, high-risk counts)
│   ├── data_quality.R           # Module theo dõi chất lượng dữ liệu (Missing rate, invalid stats)
│   ├── player_analysis.R        # Module soi chi tiết chỉ số của 1 player & so sánh với Lobby
│   └── risk_analysis.R          # Module lọc danh sách rủi ro cao & xem bằng chứng gian lận
└── www/styles.css               # CSS tùy chỉnh giao diện UI/UX
```

---

## 5. Contract Dữ liệu & Chuẩn Giao tiếp

Tất cả các service (Go, Rust, R) giao tiếp bất đồng bộ qua Kafka và MinIO dựa trên Data Contract chuẩn hóa lưu tại thư mục `contracts/`.

### 5.1. Event Envelope Scheme (`contracts/schemas/event-envelope.schema.json`)
```json
{
  "schema_version": "1.0",
  "event_id": "sha256_hash_of_record",
  "op": "data.player_stat.match_summary",
  "ingest_time": "2026-07-28T01:10:00Z",
  "match_id": "match_98234",
  "player_id": "player_1029",
  "source": {
    "provider": "kaggle",
    "dataset_id": "pubg-finish-placement-prediction",
    "source_file": "train_V2.csv",
    "record_index": 1420
  },
  "payload": {
    "kills": 12,
    "damage_dealt": 1450.5,
    "headshot_kills": 9,
    "walk_distance": 3200.1,
    "ride_distance": 0.0,
    "survival_duration": 1100.0
  }
}
```

### 5.2. Batch Manifest Schema (`contracts/schemas/batch-manifest.schema.json`)
```json
{
  "batch_id": "batch-20260728-000001",
  "source_topic": "pubg.v1.player-stat.raw",
  "kafka_partition": 0,
  "first_offset": 0,
  "last_offset": 4999,
  "record_count": 5000,
  "valid_record_count": 4920,
  "invalid_record_count": 80,
  "data_object": "bronze/player-stat/partition=0/offset=0-4999/batch.parquet",
  "checksum": "sha256-checksum-value",
  "status": "completed"
}
```

---

## 6. Luồng Triển khai & HA Architecture

Hệ thống được thiết kế để có thể chạy trên môi trường Cloud-Native (Kubernetes, MinIO Tenant, Kafka Cluster) hoặc triển khai nhanh cục bộ qua Docker Compose:

```text
+---------------------------------------------------------------------------------+
|                               Cloud-Native / Host Environment                   |
|                                                                                 |
|  +------------------+     Shared Volume     +-------------------+               |
|  |  dataset-sync    | ────────────────────> |   Local Storage   |               |
|  +------------------+                       +---------+---------+               |
|                                                       │                         |
|                                                       v                         |
|  +------------------+   JSON Events (Keyed) +-------------------+               |
|  |    go-replay     | ────────────────────> | Kafka Broker Clst |               |
|  +------------------+                       +---------+---------+               |
|                                                       │                         |
|                                                       v (Consumer Group)        |
|  +------------------+    Parquet & Manifest +-------------------+               |
|  |  rust-processor  | ────────────────────> | MinIO Object Store|               |
|  +------------------+                       +---------+---------+               |
|                                                       │                         |
|                                      ┌────────────────┼────────────────┐        |
|                                      v                v                v        |
|                               +--------------+ +--------------+ +-------------+ |
|                               | R Preprocess | |  R Scoring   | |    Shiny    | |
|                               |  & Features  | |  & Training  | |  Dashboard  | |
|                               +--------------+ +--------------+ +-------------+ |
+---------------------------------------------------------------------------------+
```

---

## 7. An toàn Dữ liệu, Race Conditions & Bảo mật

### 1. Tránh Race Condition trong Ghi Dữ liệu:
* **Unique Object Keying**: File Parquet ở Bronze được đặt tên theo quy tắc bất biến `partition={p}/offset={start}-{end}/batch.parquet`. Việc ghi lại một batch cũ sẽ overwrite chính xác object đó mà không gây mất mát hay nhầm lẫn dữ liệu giữa các worker chạy song song.
* **Deterministic Feature Partitioning**: Ở tầng Gold/Silver, dữ liệu được phân chia theo `feature_version` và `score_date`, đảm bảo các job R phân tích song song không ghi đè dữ liệu của nhau.

### 2. Idempotency & De-duplication:
* Trường `event_id` được sinh ra bằng Hashing SHA-256 các thông tin định danh trận đấu (`match_id` + `player_id` + `record_index`).
* Rust Processor lọc trùng lặp trong bộ nhớ của từng batch dựa trên `event_id` trước khi chuyển sang định dạng Arrow.

### 3. Xử lý Lỗi Logic & Data Validation:
* **Validation 2 Tầng**:
  1. *Schema Validation*: Kiểm tra sự hiện diện và kiểu dữ liệu của các trường bắt buộc trong Envelope.
  2. *Semantic Validation*: Kiểm tra tính logic của game (Ví dụ: `headshot_kills` không được vượt quá `kills`, `walk_distance` không được âm, `survival_duration` nằm trong ngưỡng trận đấu).
* Các bản ghi vi phạm Semantic/Schema sẽ tự động đẩy sang Topic `pubg.v1.invalid` hoặc thư mục `bronze/invalid/` để phục vụ audit.

### 4. Quản lý An toàn Bảo mật & Secrets:
* Không hard-code các thông tin nhạy cảm (Kaggle Credentials, MinIO Access Keys, Kafka SSL certs) trong codebase. Tất cả được nạp qua Environment Variables hoặc Kubernetes Secrets / Docker Secrets.
