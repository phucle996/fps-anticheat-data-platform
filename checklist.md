# PUBG Anti-Cheat Data Platform — MVP Phases and Tasks

## Phase 1 — Khởi tạo project

* [x] Tạo monorepo.
* [x] Tạo cấu trúc thư mục `apps`.
* [x] Tạo cấu trúc `contracts`.
* [x] Tạo cấu trúc `configs`.
* [x] Tạo cấu trúc `data`.
* [x] Tạo cấu trúc `deployments`.
* [x] Tạo cấu trúc `scripts`.
* [x] Tạo cấu trúc `tests`.
* [x] Tạo `.gitignore`.
* [x] Tạo `.env.example`.
* [x] Tạo `Makefile`.
* [x] Viết README cơ bản`.
* [x] Khởi tạo Git repository.

---

## Phase 2 — Docker Compose Infrastructure

* [x] Tạo `docker-compose.yml`.
* [x] Thêm Kafka container.
* [x] Thêm MinIO container.
* [x] Thêm MinIO Console.
* [x] Tạo Docker network.
* [x] Tạo persistent volumes.
* [x] Thêm healthcheck cho Kafka.
* [x] Thêm healthcheck cho MinIO.
* [x] Tạo script khởi tạo Kafka topics.
* [x] Tạo topic `pubg.v1.player-stat.raw`.
* [x] Tạo topic `pubg.v1.invalid`.
* [x] Tạo script khởi tạo MinIO bucket.
* [x] Tạo bucket `pubg-data`.
* [x] Kiểm tra Kafka producer/consumer bằng CLI.
* [x] Kiểm tra upload/download object trên MinIO.

---

## Phase 3 — Data Contract

* [x] Chọn Kaggle PUBG dataset.
* [x] Xác định dataset slug.
* [x] Xác định file CSV chính.
* [x] Liệt kê các columns.
* [x] Chọn columns sử dụng trong MVP.
* [x] Tạo data dictionary.
* [x] Định nghĩa event envelope.
* [x] Định nghĩa source metadata.
* [x] Định nghĩa `schema_version`.
* [x] Định nghĩa các giá trị `op`.
* [x] Định nghĩa cách tạo `event_id`.
* [x] Tạo JSON Schema cho event.
* [x] Tạo schema cho dataset manifest.
* [x] Tạo schema cho batch manifest.
* [x] Tạo schema cho prediction.
* [x] Tạo valid event example.
* [x] Tạo invalid event example.

---

## Phase 4 — Go Dataset Sync (MinIO S3 Integration)

* [x] Khởi tạo Go module.
* [x] Tạo command `dataset-sync`.
* [x] Tạo config loader.
* [x] Tạo config validation.
* [x] Đọc Kaggle credentials.
* [x] Tạo Kaggle client.
* [x] Tải dataset archive.
* [x] Lưu archive vào MinIO S3 (`pubg-data/archives/`).
* [x] Ghi file download tạm thời.
* [x] Xác nhận download hoàn thành.
* [x] Tính SHA-256 checksum.
* [x] Giải nén archive.
* [x] Lưu dữ liệu vào MinIO S3 (`pubg-data/raw-sources/`).
* [x] Kiểm tra selected CSV file tồn tại.
* [x] Tạo dataset manifest.
* [x] Lưu manifest vào MinIO S3 (`pubg-data/manifests/dataset-manifest.json`).
* [x] Bỏ qua download nếu dataset đã sẵn sàng trên MinIO.
* [x] Thêm structured JSON logging.
* [x] Dockerize `dataset-sync`.

### Phase 4 FIX bổ sung
* [x] Refactor `main.go` thành thuần Entrypoint (Log init, Graceful shutdown handling).
* [x] Tạo `internal/app/dataset_sync.go` điều phối Use Case ứng dụng.
* [x] Chuyển Dockerfile runtime sang Google Distroless (`gcr.io/distroless/static-debian12`).
* [x] Cập nhật Go version trong `go.mod`.

---

## Phase 5 — Go CSV Parser

* [x] Tạo parser interface.
* [x] Tạo CSV parser implementation.
* [x] Đọc CSV header.
* [x] Đọc CSV theo streaming.
* [x] Không load toàn bộ file vào RAM.
* [x] Tạo raw record struct.
* [x] Tạo column mapping.
* [x] Gắn source file.
* [x] Gắn record index.
* [x] Xử lý empty row.
* [x] Xử lý malformed CSV row.
* [x] Tạo sample CSV.
* [x] Viết unit test cho parser.

### Phase 5 FIX bổ sung
* [x] Gom toàn bộ CSV Parser thành file duy nhất (`internal/parser/csv.go`).
* [x] Tổ chức tập trung unit tests & testdata vào thư mục test chung (`internal/test/parser/`).

---

## Phase 6 — Go Normalization và Validation

* [x] Tạo normalizer interface.
* [x] Tạo player-stat normalizer.
* [x] Chuẩn hóa player ID.
* [x] Chuẩn hóa match ID.
* [x] Parse integer fields.
* [x] Parse floating-point fields.
* [x] Chuẩn hóa missing values.
* [x] Tạo event payload.
* [x] Tạo ingest timestamp.
* [x] Tạo deterministic event ID.
* [x] Tạo event envelope.
* [x] Validate required fields.
* [x] Validate numeric parsing.
* [x] Tạo invalid record structure.
* [x] Viết unit test cho normalizer.
* [x] Viết unit test cho event ID.
* [x] Viết unit test cho validation.

---

## Phase 7 — Go Dataset Replay

* [x] Tạo command `replay`.
* [x] Load dataset manifest.
* [x] Xác định selected CSV.
* [x] Khởi tạo CSV parser.
* [x] Khởi tạo normalizer.
* [x] Tạo replay loop.
* [x] Hỗ trợ giới hạn số records.
* [x] Hỗ trợ start record.
* [x] Hỗ trợ dry-run.
* [x] In normalized event trong dry-run.
* [x] Theo dõi replay statistics.
* [x] Đếm records read.
* [x] Đếm valid records.
* [x] Đếm invalid records.
* [x] Đếm produced records.
* [x] Thêm graceful shutdown.

### Phase 7 FIX bổ sung
* [x] Refactor go-ingestor sang Flat Architecture (gom toàn bộ core logic vào `internal/service/` và unit tests vào `internal/test/service/`).

---

## Phase 8 — Go Kafka Producer

* [x] Tạo producer interface.
* [x] Tạo Kafka producer implementation.
* [x] Load Kafka broker configuration.
* [x] Serialize event thành JSON.
* [x] Dùng `match_id` làm message key.
* [x] Route valid event vào raw topic.
* [x] Route invalid event vào invalid topic.
* [x] Cấu hình `acks=all`.
* [x] Cấu hình retry.
* [x] Cấu hình Zstandard compression.
* [x] Bật idempotent producer.
* [x] Xử lý delivery result.
* [x] Xử lý Kafka unavailable.
* [x] Đóng producer khi shutdown.
* [x] Viết Kafka integration test.

### Phase 8 FIX bổ sung
* [x] Áp dụng nguyên tắc Fail-Close / Fail-Fast 100% cho cấu hình (Zero Fallback).
* [x] Chuyển toàn bộ trách nhiệm validate tham số nil/rỗng về duy nhất lớp Thượng nguồn (Transport Config Layer), hạ nguồn không re-validate trùng lặp.

---

## Phase 9 — Go Micro-Batching

* [x] Tạo topic buffer.
* [x] Tạo batch structure.
* [x] Theo dõi record count.
* [x] Theo dõi estimated bytes.
* [x] Tạo flush controller.
* [x] Flush theo thời gian.
* [x] Flush theo record count.
* [x] Flush theo batch bytes.
* [x] Flush khi hết file.
* [x] Flush khi shutdown.
* [x] Không đóng gói event thành JSON array.
* [x] Chỉ cập nhật statistics sau delivery thành công.
* [x] Viết unit test cho batch boundary.
* [x] Viết unit test cho timer flush.
* [x] Viết unit test cho end-of-file flush.

### Phase 9 FIX bổ sung
* [x] Tinh chỉnh Micro-Batching về ngưỡng siêu nhỏ (MaxBatchSize=20, MaxBatchBytes=16KB, FlushInterval=500ms) tối ưu TCP I/O & Zstd bandwidth, nhường việc batch lớn cho Rust engine.

---

## Phase 10 — Go Replay Checkpoint (MinIO S3 Store)

* [x] Tạo checkpoint structure.
* [x] Tạo checkpoint store interface.
* [x] Tạo MinIO S3 checkpoint store (`pubg-data/checkpoints/go-replay/state.json`).
* [x] Lưu dataset ID.
* [x] Lưu source file.
* [x] Lưu last completed record index.
* [x] Lưu updated timestamp.
* [x] Load checkpoint từ MinIO khi replay bắt đầu.
* [x] Resume đọc CSV từ checkpoint record index.
* [x] Chỉ lưu checkpoint sau Kafka acknowledgement.
* [x] Thêm option disable checkpoint.
* [x] Thêm option reset checkpoint trên MinIO.
* [x] Viết unit test cho MinIO checkpoint store.
* [x] Kiểm tra replay resume từ MinIO.

### Phase 10 FIX bổ sung
* [x] Bổ sung `RemoveObject` vào `MinIOClient` cho tính năng xóa vật lý Checkpoint trên MinIO S3.
* [x] Chuẩn hóa compile-time interface assertion `var _ CheckpointStore = (*MinIOCheckpointStore)(nil)`.
* [x] Bổ sung `sync.Once` trong `StopTimer()` phòng ngừa panic closure channel khi shutdown.

---

## Phase 11 — Rust Project Foundation

* [x] Khởi tạo Rust project.
* [x] Tạo module structure.
* [x] Tạo config loader.
* [x] Tạo structured logging.
* [x] Định nghĩa event envelope struct.
* [x] Định nghĩa batch metadata.
* [x] Định nghĩa application errors.
* [x] Thêm Kafka client dependency.
* [x] Thêm Arrow dependency.
* [x] Thêm Parquet dependency.
* [x] Thêm S3/MinIO client dependency.
* [x] Tạo Dockerfile.
* [x] Thêm Rust Processor vào Docker Compose.

---

## Phase 12 — Rust Kafka Consumer

* [x] Kết nối Kafka broker.
* [x] Tạo consumer group.
* [x] Subscribe raw topic.
* [x] Tắt automatic offset commit.
* [x] Poll Kafka messages.
* [x] Đọc topic metadata.
* [x] Đọc partition.
* [x] Đọc offset.
* [x] Deserialize JSON event.
* [x] Xử lý malformed JSON.
* [x] Đưa event vào Batch Accumulator.
* [x] Xử lý partition revoke.
* [x] Xử lý graceful shutdown.
* [x] Viết consumer integration test.

---

## Phase 13 — Rust Batch Accumulator

* [x] Tạo accumulator.
* [x] Gom event theo partition.
* [x] Theo dõi first offset.
* [x] Theo dõi last offset.
* [x] Theo dõi record count.
* [x] Theo dõi estimated bytes.
* [x] Flush theo timer.
* [x] Flush theo record count.
* [x] Flush theo batch bytes.
* [x] Flush khi partition revoke.
* [x] Flush khi shutdown.
* [x] Tạo batch ID.
* [x] Viết unit test cho accumulator.

### Phase 13 FIX bổ sung
* [x] Quy hoạch cấu trúc module `src/ingest/` (chứa `consumer.rs` và `accumulator.rs`) gom cụm toàn bộ luồng Ingestion theo chuẩn Domain-Driven / Pipeline Architecture.

---

## Phase 14 — Rust Data Quality

* [x] Validate schema version.
* [x] Validate event ID.
* [x] Validate operation.
* [x] Validate match ID.
* [x] Validate player ID.
* [x] Validate payload structure.
* [x] Validate kills.
* [x] Validate damage.
* [x] Validate movement distance.
* [x] Validate survival duration.
* [x] Validate headshot kills.
* [x] Đếm valid records.
* [x] Đếm invalid records.
* [x] Ghi validation reason.
* [x] Viết unit test cho từng validation rule.

---

## Phase 15 — Rust Deduplication

* [x] Deduplicate theo `event_id`.
* [x] Deduplicate trong processing batch.
* [x] Đếm duplicate records.
* [x] Giữ một record hợp lệ.
* [x] Ghi duplicate count vào metadata.
* [x] Viết unit test cho duplicate event.
* [x] Viết test cho batch không có duplicate.

---

## Phase 16 — Rust Arrow và Parquet

* [x] Định nghĩa Arrow schema.
* [x] Map event metadata thành columns.
* [x] Map payload thành columns.
* [x] Xử lý nullable columns.
* [x] Tạo Arrow arrays.
* [x] Tạo Arrow RecordBatch.
* [x] Serialize RecordBatch thành Parquet.
* [x] Cấu hình Zstandard compression.
* [x] Tạo local Parquet test file.
* [x] Đọc lại Parquet để kiểm tra.
* [x] Viết unit test cho schema transformation.
* [x] Viết unit test cho Parquet output.

---

## Phase 17 — Rust MinIO Bronze Writer

* [x] Tạo MinIO client.
* [x] Load endpoint configuration.
* [x] Load access key và secret key.
* [x] Kiểm tra bucket tồn tại.
* [x] Tạo Bronze object path.
* [x] Upload Parquet file.
* [x] Kiểm tra object đã tồn tại.
* [x] Tính checksum.
* [x] Xác nhận upload thành công.
* [x] Xử lý upload retry.
* [x] Ghi invalid data nếu cần.
* [x] Viết MinIO integration test.

---

## Phase 18 — Rust Batch Manifest và Offset Commit

* [x] Định nghĩa batch manifest.
* [x] Ghi source topic.
* [x] Ghi partition.
* [x] Ghi first offset.
* [x] Ghi last offset.
* [x] Ghi record counts.
* [x] Ghi data object path.
* [x] Ghi checksum.
* [x] Ghi processing timestamp.
* [x] Upload manifest lên MinIO.
* [x] Chỉ commit offset sau Parquet upload.
* [x] Chỉ commit offset sau manifest upload.
* [x] Xử lý retry khi commit lỗi.
* [x] Kiểm tra idempotent object path.
* [x] Viết integration test cho durable write.

---

## Phase 19 — MinIO Data Lake Structure

* [x] Tạo `bronze/player-stat`.
* [x] Tạo `bronze/invalid`.
* [x] Tạo `manifests`.
* [x] Tạo `silver/players`.
* [x] Tạo `silver/matches`.
* [x] Tạo `silver/player-match`.
* [x] Tạo `gold/player-match-features`.
* [x] Tạo `models`.
* [x] Tạo `predictions`.
* [x] Chốt object naming convention.
* [x] Chốt partitioning convention.
* [x] Viết tài liệu data lake layout.

---

## Phase 20 — R Project Foundation

* [x] Khởi tạo R project (`apps/r-processor`).
* [x] Khởi tạo `renv` quản lý R packages độc lập.
* [x] Tạo thư mục `R`.
* [x] Tạo thư mục `scripts`.
* [x] Tạo config loader cho R.
* [x] Tạo MinIO storage module cho R.
* [x] Tạo manifest reader.
* [x] Thêm Arrow package (`arrow`).
* [x] Thêm data-processing packages (`dplyr`, `data.table`).
* [x] Thêm model package (`solitude` / `isotree`).
* [x] Thêm testing package (`testthat`).
* [x] Tích hợp Async Subprocess Spawner trong Rust Processor (`tokio::process::Command`, Semaphore concurrency control).
* [x] Đóng gói R Runtime và renv packages vào Docker Container environment.

---

## Phase 21 — R Preprocessing và Silver

* [x] Hỗ trợ nhận tham số `manifest_path` từ Rscript Subprocess CLI.
* [x] List Bronze manifests.
* [x] Chỉ đọc manifest completed.
* [x] Đọc Bronze Parquet.
* [x] Kiểm tra schema.
* [x] Kiểm tra checksum nếu cần.
* [x] Chuẩn hóa column types.
* [x] Xử lý missing values.
* [x] Loại duplicate toàn cục theo `event_id` giữa các Bronze batches.
* [x] Tạo bảng players.
* [x] Tạo bảng matches.
* [x] Tạo bảng player-match.
* [x] Ghi Silver Parquet.
* [x] Ghi processing status.
* [x] Tạo data-quality summary.
* [x] Viết test cho preprocessing.

### phase 21 fix bổ sung 
* [x] Thiết lập Spark-style Dynamic R Worker Pool Daemon (Persistent process IPC/stdin loop).
* [x] Tích hợp bộ Ticker & Idle Timer (5s timeout auto-shutdown cho R worker khi nhàn rỗi).
* [x] Tối ưu hóa tái sử dụng R process (Warm process reuse, 0ms startup latency khi stream dữ liệu liên tục).
* [x] Thiết lập Pure Resource-Driven Auto-Scaling (Bỏ hardcode max worker, đo chỉ số CPU/RAM thực tế theo thời gian thực).
* [x] Xây dựng CPU Resource Circuit Breaker với Hysteresis Gap (Tạm dừng spawn R Worker khi CPU >= 80%, cho phép spawn lại khi CPU <= 75%).
* [x] Cấu hình Docker Compose Resource Limits (`cpus`, `memory`) dành riêng cho Rust & R Processors.

---

## Phase 22 — R Feature Engineering và Gold

* [x] Chọn feature columns.
* [x] Tính total distance.
* [x] Tính headshot ratio.
* [x] Tính kills per minute.
* [x] Tính damage per minute.
* [x] Tính damage per kill.
* [x] Tính movement per minute.
* [x] Tính performance versus lobby.
* [x] Xử lý chia cho 0.
* [x] Xử lý `NA`.
* [x] Xử lý giá trị vô hạn.
* [x] Validate feature types.
* [x] Tạo feature schema.
* [x] Tạo feature version.
* [x] Ghi Gold Parquet.
* [x] Viết test cho từng feature.

---

## Phase 23 — Exploratory Data Analysis

* [x] Thống kê số player.
* [x] Thống kê số match.
* [x] Thống kê kills.
* [x] Thống kê damage.
* [x] Thống kê headshot ratio.
* [x] Thống kê movement.
* [x] Vẽ feature distributions.
* [x] Kiểm tra missing values.
* [x] Kiểm tra feature correlation.
* [x] Kiểm tra extreme values.
* [x] Chọn feature dùng cho model.
* [x] Xuất EDA report.

---

## Phase 24 — Python ML Worker Training và ONNX Export

* [x] Khởi tạo ứng dụng Python ML Worker (`apps/python-ml-worker`).
* [x] Subscribe Kafka event `pubg.v1.dataset.gold.ready`.
* [x] Nạp Gold Feature Parquet từ MinIO S3.
* [x] Chia tập Train/Validation/Test (Group split theo `match_id` / `player_id_hash`).
* [x] Train mô hình Logistic Regression làm baseline.
* [x] Train mô hình Random Forest & HistGradientBoosting.
* [x] Train mô hình Unsupervised Isolation Forest.
* [x] Đánh giá mô hình theo PR-AUC, Precision, Recall, F1-Score.
* [x] Đóng gói ONNX Model Bundle (`model.onnx`, `feature_schema.json`, `threshold_policy.json`).
* [x] Upload ONNX Model Bundle lên MinIO `s3://pubg-models/`.
* [x] Publish Kafka event `pubg.v1.ml.model.ready`.
* [x] Viết unit test cho Python ML Training & ONNX Export.

### phase 24 fix bổ sung
* [x] Refactor di chuyển `apps/python-ml-worker` vào `apps/ml-platform/python-ml-worker` chuẩn hóa kiến trúc 3-in-1 Unified ML Platform Container.

---

## Phase 25 — Dedicated Rust ONNX Runtime Engine (`apps/ml-platform/rust-inference/`)

* [x] Khởi tạo module Rust ML/AI Inference (`apps/ml-platform/rust-inference`).
* [x] Tích hợp ONNX Runtime C-bindings (`ort` crate) vào `rust-inference`.
* [x] Nhận tín hiệu IPC trực tiếp từ Python ML Worker khi model mới sẵn sàng (Zero Kafka overhead nội bộ).
* [x] Đọc trực tiếp và verify checksum ONNX Model Bundle từ local shared directory (`models/v1/`).
* [x] Atomic Hot-Swap mô hình ONNX trong RAM (Zero Downtime).
* [x] Chạy Tensor Inference trực tiếp cho tập dữ liệu Gold features.
* [x] Viết unit test cho Rust ONNX Engine trong `apps/ml-platform/rust-inference`.


---

## Phase 26 — Rust Unix Domain Socket IPC Server (`apps/ml-platform/rust-inference/src/ipc/`)

* [x] Thiết lập Unix Domain Socket Listener (`/tmp/rust_inference.sock`) trong `rust-inference`.
* [x] Xử lý giao thức IPC Request/Response JSON siêu tốc với Go API Gateway (Thuần IPC 100%).
* [x] Chuẩn hóa Anomaly Risk Score (0.0 - 1.0) và gán nhãn Risk Level.
* [x] Ghi kết quả Predictions Parquet lên MinIO `s3://pubg-predictions/`.
* [x] Viết unit test cho Rust UDS IPC Server.

---

## Phase 27 — Go API Gateway Service (`apps/ml-platform/go-api/`)

* [x] Khởi tạo module Go API Gateway (`apps/ml-platform/go-api`).
* [x] Thêm biến môi trường cấu hình Fail-Close cho Go API Gateway.
* [x] Xây dựng HTTP REST Endpoints (`/api/v1/health`, `/api/v1/predict`, `/api/v1/dataset/summary`).
* [x] Kết nối Unix Domain Socket IPC client (`/tmp/rust_inference.sock`) tới `rust-inference`.
* [x] Viết integration test cho Go API Gateway.

---

## Phase 28 — Prediction Evidence và Model Parity Verification

* [x] Viết Model Parity Test (Xác nhận Python ONNX output ≈ Rust ONNX output ≤ 1e-5).
* [x] Tính toán Robust Z-Score và Feature Percentile so với trung bình Lobby.
* [x] Trích xuất Top Evidence Features nghi vấn gian lận cho từng player.
* [x] Đóng gói Weak-label Risk Score và Evidence Matrix vào Prediction API Response.
* [x] Viết unit test cho Evidence Generation.

### Hot-fix — Model Persistence & Automatic MinIO Recovery

* [x] Tự động upload file `model.onnx` lên MinIO S3 `pubg-models/v1/model.onnx` sau khi Python ML Worker xuất model.
* [x] Tự động khôi phục (download) `model.onnx` từ MinIO S3 khi Rust Inference Engine khởi động/restart mà đĩa local chưa có file.
* [x] Cập nhật sơ đồ kiến trúc trong `apps/ml-platform/README.md`.

---

## Phase 29 — Streamlit Dashboard Foundation (`apps/streamlit-dashboard`)

* [x] Khởi tạo Streamlit Application (`apps/streamlit-dashboard`).
* [x] Cấu hình kết nối MinIO S3 Data Lake & Go API Gateway.
* [x] Tạo navigation layout đa trang và Dockerfile cho Streamlit.
* [x] Bổ sung `streamlit-dashboard` vào Docker Compose.

---

## Phase 30 — Streamlit Overview & Health Page

* [x] Hiển thị tổng số records.
* [x] Hiển thị tổng số matches.
* [x] Hiển thị tổng số players.
* [x] Hiển thị tổng số batches.
* [x] Hiển thị valid record count.
* [x] Hiển thị invalid record count.
* [x] Hiển thị prediction count.
* [x] Hiển thị high-risk count.
* [x] Hiển thị model version.
* [x] Hiển thị feature version.

---

## Phase 31 — Streamlit Preprocessing Before vs After Page

* [x] Hiển thị missing values.
* [x] Hiển thị duplicate records.
* [x] Hiển thị invalid records.
* [x] Hiển thị invalid reasons.
* [x] Hiển thị record count theo batch.
* [x] Biểu đồ so sánh 1-1 trước và sau khi làm sạch dữ liệu.
* [x] Thêm filter theo source file và batch.

---

## Phase 32 — Streamlit Player Analysis Page

* [x] Tạo player selector.
* [x] Hiển thị match history.
* [x] Hiển thị kills, damage, headshot ratio, movement.
* [x] So sánh chỉ số người chơi với trung bình Lobby.
* [x] Hiển thị risk score.
* [x] Hiển thị prediction evidence.

---

## Phase 33 — Streamlit Risk Analysis Page

* [x] Hiển thị prediction table.
* [x] Sort theo risk score.
* [x] Filter theo risk level và model version.
* [x] Hiển thị score distribution.
* [x] Hiển thị prediction details và top evidence features.

---

## Phase 34 — End-to-End System Integration

* [ ] Chạy Kafka và MinIO.
* [ ] Chạy Dataset Sync.
* [ ] Kiểm tra dataset manifest.
* [ ] Chạy Rust Processor.
* [ ] Chạy R Preprocessing (Silver & Gold).
* [ ] Chạy Python Model Training.
* [ ] Kiểm tra ONNX model artifacts.
* [ ] Chạy Rust ONNX Engine (UDS IPC).
* [ ] Chạy Go API Gateway.
* [ ] Mở Streamlit Dashboard.
* [ ] Kiểm tra toàn bộ pipeline end-to-end.

---

## Phase 35 — Comprehensive Testing Suite

* [ ] Go API unit & integration tests.
* [ ] Rust ONNX & IPC unit tests.
* [ ] R preprocessing & feature tests.
* [ ] Python ML training & export tests.
* [ ] End-to-end failure tests.

---

## Phase 36 — Logging, Documentation và Demo Setup

* [ ] Chuẩn hóa log format và OTel log routing.
* [ ] Hoàn thiện architecture document & data dictionary.
* [ ] Tạo demo sample dataset và `make demo` command.
* [ ] Chuẩn bị screenshots và video demo dự phòng.

---

## Phase 37 — Refactor Go Ingestor Real-Time Stream Replay Engine

### Tái cấu trúc Go Dataset Ingestor sang luồng phát sự kiện rải rác thời gian thực (Real-Time Event Stream Replay)
* [ ] Tải 1 lần file Kaggle dataset gốc về đĩa đệm local (`data/raw/`).
* [ ] Cấu hình Streaming CSV Line-by-Line Reader (`bufio.Scanner`) nạp dữ liệu rải rác.
* [ ] Thêm Ticker & Rate Limiter giả lập khoảng trễ tự nhiên giữa các bản ghi game events (`10ms - 50ms Jitter`).
* [ ] Giữ nguyên kích thước batch hiện tại và duy trì bộ nhớ đệm RAM của Go Ingestor siêu nhẹ (< 15MB RAM).
