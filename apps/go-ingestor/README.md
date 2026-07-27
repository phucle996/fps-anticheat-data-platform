# Go Dataset Ingestor Service (`apps/go-ingestor`)

Dịch vụ **Go Dataset Ingestor** đóng vai trò là cổng nạp dữ liệu thô (Raw Data Ingestor) trung tâm cho toàn bộ nền tảng **PUBG PC Anti-Cheat Data Platform**. 

Ứng dụng tải/nạp các tập dữ liệu PUBG PC Telemetry từ Kaggle/CSV, thực hiện chuẩn hóa dữ liệu thô sang **Canonical Event Envelope Schema**, kiểm tra validation cơ bản và đẩy luồng dữ liệu sự kiện vào **Kafka Raw Topic (`pubg.v1.player-stat.raw`)**.

---

## 🏛️ Kiến Trúc và Luồng Dữ Liệu (Architecture & Flow)

```text
[ Kaggle PUBG Dataset / Local CSV ]
                 │
                 ▼
     [ Go Dataset Ingestor ]
     ├── 1. Kaggle Downloader / CSV File Reader
     ├── 2. Canonical Normalizer (Hash player_id & match_id)
     ├── 3. Base Validator (Rule check schema & bounds)
     └── 4. Kafka Sync / Replay Producer Daemon
                 │
                 ▼
  [ Kafka Raw Topic: pubg.v1.player-stat.raw ]
```

---

## 🚀 Tính Năng Nổi Bật (Core Features)

1. **Kaggle PUBG Telemetry Downloader**:
   - Tự động tải các tập dữ liệu PUBG PC public dataset chính thức thông qua Kaggle API.
2. **Canonical Schema Normalization**:
   - Chuẩn hóa tên trường, băm bảo mật định danh người chơi và trận đấu bằng SHA-256 (`player_id_hash`, `match_id`).
3. **Fail-Close 100% Environment Configuration**:
   - Nạp biến môi trường bắt buộc, tự động ném ra lỗi `ValueError/Error` ngắt ứng dụng ngay tức thì nếu thiếu bất kỳ biến cấu hình nào (Zero Default Fallback).
4. **Base Rule Validation**:
   - Lọc các bản ghi lỗi nghiêm trọng (`kills < 0`, `win_place_perc < 0.0` hoặc `> 1.0`, `headshot_kills > total_kills`).
5. **Batching & Checkpoint Resumption**:
   - Gom dữ liệu theo từng Batch (ví dụ 400 bản ghi/batch), ghi nhận Checkpoint vị trí đọc để hỗ trợ Replay và phục hồi lỗi (Fault Tolerance).

---

## ⚙️ Cấu Hình Biến Môi Trường (Fail-Close Enforced)

| Biến Môi Trường | Mô Tả | Ví Dụ |
| :--- | :--- | :--- |
| `KAFKA_BROKERS` | Danh sách Kafka Brokers | `localhost:9092` |
| `KAFKA_TOPIC_RAW` | Topic Kafka nạp dữ liệu thô | `pubg.v1.player-stat.raw` |
| `KAGGLE_USERNAME` | Kaggle API Username | `my_kaggle_user` |
| `KAGGLE_KEY` | Kaggle API Token Key | `1234567890abcdef` |
| `MINIO_ENDPOINT` | Endpoint S3 MinIO Object Storage | `http://localhost:9000` |
| `MINIO_BUCKET_DATA` | Bucket chứa Data Lake Parquet | `fps-anticheat-datalake` |
| `MINIO_ACCESS_KEY` | Access Key MinIO | `minioadmin` |
| `MINIO_SECRET_KEY` | Secret Key MinIO | `minioadmin` |

---

## 🛠️ Hướng Dẫn Chạy Dịch Vụ (Execution Commands)

### 1. Kiểm thử Unit Test Suite
```bash
cd apps/go-ingestor
go test -v ./...
```

### 2. Khởi chạy Sync Dữ Liệu Từ Kaggle
```bash
cd apps/go-ingestor
go run cmd/sync/main.go
```

### 3. Khởi chạy Replay Daemon Đẩy Sự Kiện Vào Kafka
```bash
cd apps/go-ingestor
go run cmd/replay/main.go
```

---

## 📂 Cấu Trúc Thư Mục (Directory Structure)

```text
apps/go-ingestor/
├── cmd/
│   ├── sync/main.go           # CLI entrypoint đồng bộ dữ liệu Kaggle
│   └── replay/main.go         # CLI entrypoint Replay Daemon đẩy Kafka
├── internal/
│   ├── config/config.go       # Fail-Close Environment Loader
│   ├── normalize/             # Normalizer băm ID & chuẩn hóa canonical schema
│   ├── service/               # Core services (Checkpoint, Kaggle, Batching, Kafka Producer)
│   └── test/                  # Unit & Integration test suites
├── go.mod                     # Go module definitions
└── README.md                  # Hướng dẫn chi tiết dịch vụ
```
