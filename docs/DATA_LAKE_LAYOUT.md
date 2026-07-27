# PUBG Anti-Cheat Medallion Data Lake Layout & Specifications

Tài liệu quy định kiến trúc lưu trữ dữ liệu dạng cột **Medallion Data Lake Architecture** trên MinIO S3 Object Storage (`fps-anticheat-datalake` bucket) phục vụ hệ thống **FPS Anti-Cheat Data Platform**.

---

## 🏗️ Tổng quan Kiến trúc Medallion Data Lake

Hệ thống lưu trữ dữ liệu được chia làm 4 tầng độc lập theo tiêu chuẩn Data Lake Cloud-Native:

```text
fps-anticheat-datalake/
├── bronze/                         # Raw Ingestion Layer (Parquet & JSON thô từ Ingestor)
│   ├── player-stat/                # Các bản ghi thống kê trận đấu hợp lệ (Parquet)
│   └── invalid/                    # Các bản ghi vi phạm Data Quality Validation (JSON)
├── manifests/                      # Audit Log Manifests của từng Processing Batch (JSON)
├── silver/                         # Cleaned & Curated Entities Layer
│   ├── players/                    # Hồ sơ thông tin tích lũy của từng Player
│   ├── matches/                    # Thống kê tổng quan từng trận đấu PUBG
│   └── player-match/               # Thống kê chi tiết Player theo từng Match
├── gold/                           # Feature Store Layer cho ML & Analytics
│   └── player-match-features/      # Bảng đặc trưng (Feature Matrix) chuẩn bị cho R ML Model
├── models/                         # ML Model Artifacts Registry (R XGBoost / Random Forest Models)
└── predictions/                    # Anti-Cheat Anomaly Detection Scoring Results
```

---

## 🏷️ Quy ước Phân vùng (Hive Partitioning Standard)

Tất cả dữ liệu theo chuỗi thời gian (Time-series / Event-driven) trên MinIO S3 phải tuân thủ nghiêm ngặt định dạng **Hive Partitioning**:

$$\text{path} = \text{layer}/\text{entity}/\text{year}=YYYY/\text{month}=MM/\text{day}=DD/\text{file\_name}$$

### Ví dụ:
- **Bronze Event Stream**: `bronze/player-stat/year=2026/month=07/day=28/pubg_player_stat_batch_a6793109.parquet`
- **Data Quality Invalid Log**: `bronze/invalid/year=2026/month=07/day=28/pubg_invalid_batch_a6793109.json`
- **Batch Manifest Audit Log**: `manifests/year=2026/month=07/day=28/manifest_batch_a6793109.json`
- **Gold Feature Matrix**: `gold/player-match-features/year=2026/month=07/day=28/features_match_100.parquet`

---

## 📝 Quy ước Đặt tên File (Object Naming Conventions)

1. **Bronze Parquet File**: `pubg_player_stat_{batch_id}.parquet`
   - Thuật toán nén: **Zstandard (`Compression::ZSTD`)**
   - Đòn bẩy bộ nhớ: Apache Arrow RecordBatch 19 cột.
2. **Invalid Data Log File**: `pubg_invalid_{batch_id}.json`
   - Ghi nhận chi tiết danh sách lý do vi phạm (`validation_reasons`).
3. **Batch Manifest File**: `manifest_{batch_id}.json`
   - Ghi nhận audit log: `source_topic`, `partition_offsets`, `total_records`, `checksum_sha256`.
4. **Silver & Gold Parquet Files**: `{entity}_{partition_key}_{timestamp}.parquet`

---

## 🔒 Quản lý Bền vững Dữ liệu & Anti-Cheat Lineage

1. **Fail-Close Ingestion**: Mọi tiến trình ghi dữ liệu lên MinIO S3 đều yêu cầu xác thực checksum SHA-256 trước khi commit offset Kafka.
2. **Idempotency Guarantee**: Ghi đè định hạn trùng khớp theo `batch_id` giúp tránh việc nhân bản dữ liệu khi worker replay stream.
