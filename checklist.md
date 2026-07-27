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

---

## Phase 8 — Go Kafka Producer

* [ ] Tạo producer interface.
* [ ] Tạo Kafka producer implementation.
* [ ] Load Kafka broker configuration.
* [ ] Serialize event thành JSON.
* [ ] Dùng `match_id` làm message key.
* [ ] Route valid event vào raw topic.
* [ ] Route invalid event vào invalid topic.
* [ ] Cấu hình `acks=all`.
* [ ] Cấu hình retry.
* [ ] Cấu hình Zstandard compression.
* [ ] Bật idempotent producer.
* [ ] Xử lý delivery result.
* [ ] Xử lý Kafka unavailable.
* [ ] Đóng producer khi shutdown.
* [ ] Viết Kafka integration test.

---

## Phase 9 — Go Micro-Batching

* [ ] Tạo topic buffer.
* [ ] Tạo batch structure.
* [ ] Theo dõi record count.
* [ ] Theo dõi estimated bytes.
* [ ] Tạo flush controller.
* [ ] Flush theo thời gian.
* [ ] Flush theo record count.
* [ ] Flush theo batch bytes.
* [ ] Flush khi hết file.
* [ ] Flush khi shutdown.
* [ ] Không đóng gói event thành JSON array.
* [ ] Chỉ cập nhật statistics sau delivery thành công.
* [ ] Viết unit test cho batch boundary.
* [ ] Viết unit test cho timer flush.
* [ ] Viết unit test cho end-of-file flush.

---

## Phase 10 — Go Replay Checkpoint (MinIO S3 Store)

* [ ] Tạo checkpoint structure.
* [ ] Tạo checkpoint store interface.
* [ ] Tạo MinIO S3 checkpoint store (`pubg-data/checkpoints/go-replay/state.json`).
* [ ] Lưu dataset ID.
* [ ] Lưu source file.
* [ ] Lưu last completed record index.
* [ ] Lưu updated timestamp.
* [ ] Load checkpoint từ MinIO khi replay bắt đầu.
* [ ] Resume đọc CSV từ checkpoint record index.
* [ ] Chỉ lưu checkpoint sau Kafka acknowledgement.
* [ ] Thêm option disable checkpoint.
* [ ] Thêm option reset checkpoint trên MinIO.
* [ ] Viết unit test cho MinIO checkpoint store.
* [ ] Kiểm tra replay resume từ MinIO.

---

## Phase 11 — Rust Project Foundation

* [ ] Khởi tạo Rust project.
* [ ] Tạo module structure.
* [ ] Tạo config loader.
* [ ] Tạo structured logging.
* [ ] Định nghĩa event envelope struct.
* [ ] Định nghĩa batch metadata.
* [ ] Định nghĩa application errors.
* [ ] Thêm Kafka client dependency.
* [ ] Thêm Arrow dependency.
* [ ] Thêm Parquet dependency.
* [ ] Thêm S3/MinIO client dependency.
* [ ] Tạo Dockerfile.
* [ ] Thêm Rust Processor vào Docker Compose.

---

## Phase 12 — Rust Kafka Consumer

* [ ] Kết nối Kafka broker.
* [ ] Tạo consumer group.
* [ ] Subscribe raw topic.
* [ ] Tắt automatic offset commit.
* [ ] Poll Kafka messages.
* [ ] Đọc topic metadata.
* [ ] Đọc partition.
* [ ] Đọc offset.
* [ ] Deserialize JSON event.
* [ ] Xử lý malformed JSON.
* [ ] Đưa event vào Batch Accumulator.
* [ ] Xử lý partition revoke.
* [ ] Xử lý graceful shutdown.
* [ ] Viết consumer integration test.

---

## Phase 13 — Rust Batch Accumulator

* [ ] Tạo accumulator.
* [ ] Gom event theo partition.
* [ ] Theo dõi first offset.
* [ ] Theo dõi last offset.
* [ ] Theo dõi record count.
* [ ] Theo dõi estimated bytes.
* [ ] Flush theo timer.
* [ ] Flush theo record count.
* [ ] Flush theo batch bytes.
* [ ] Flush khi partition revoke.
* [ ] Flush khi shutdown.
* [ ] Tạo batch ID.
* [ ] Viết unit test cho accumulator.

---

## Phase 14 — Rust Data Quality

* [ ] Validate schema version.
* [ ] Validate event ID.
* [ ] Validate operation.
* [ ] Validate match ID.
* [ ] Validate player ID.
* [ ] Validate payload structure.
* [ ] Validate kills.
* [ ] Validate damage.
* [ ] Validate movement distance.
* [ ] Validate survival duration.
* [ ] Validate headshot kills.
* [ ] Đếm valid records.
* [ ] Đếm invalid records.
* [ ] Ghi validation reason.
* [ ] Viết unit test cho từng validation rule.

---

## Phase 15 — Rust Deduplication

* [ ] Deduplicate theo `event_id`.
* [ ] Deduplicate trong processing batch.
* [ ] Đếm duplicate records.
* [ ] Giữ một record hợp lệ.
* [ ] Ghi duplicate count vào metadata.
* [ ] Viết unit test cho duplicate event.
* [ ] Viết test cho batch không có duplicate.

---

## Phase 16 — Rust Arrow và Parquet

* [ ] Định nghĩa Arrow schema.
* [ ] Map event metadata thành columns.
* [ ] Map payload thành columns.
* [ ] Xử lý nullable columns.
* [ ] Tạo Arrow arrays.
* [ ] Tạo Arrow RecordBatch.
* [ ] Serialize RecordBatch thành Parquet.
* [ ] Cấu hình Zstandard compression.
* [ ] Tạo local Parquet test file.
* [ ] Đọc lại Parquet để kiểm tra.
* [ ] Viết unit test cho schema transformation.
* [ ] Viết unit test cho Parquet output.

---

## Phase 17 — Rust MinIO Bronze Writer

* [ ] Tạo MinIO client.
* [ ] Load endpoint configuration.
* [ ] Load access key và secret key.
* [ ] Kiểm tra bucket tồn tại.
* [ ] Tạo Bronze object path.
* [ ] Upload Parquet file.
* [ ] Kiểm tra object đã tồn tại.
* [ ] Tính checksum.
* [ ] Xác nhận upload thành công.
* [ ] Xử lý upload retry.
* [ ] Ghi invalid data nếu cần.
* [ ] Viết MinIO integration test.

---

## Phase 18 — Rust Batch Manifest và Offset Commit

* [ ] Định nghĩa batch manifest.
* [ ] Ghi source topic.
* [ ] Ghi partition.
* [ ] Ghi first offset.
* [ ] Ghi last offset.
* [ ] Ghi record counts.
* [ ] Ghi data object path.
* [ ] Ghi checksum.
* [ ] Ghi processing timestamp.
* [ ] Upload manifest lên MinIO.
* [ ] Chỉ commit offset sau Parquet upload.
* [ ] Chỉ commit offset sau manifest upload.
* [ ] Xử lý retry khi commit lỗi.
* [ ] Kiểm tra idempotent object path.
* [ ] Viết integration test cho durable write.

---

## Phase 19 — MinIO Data Lake Structure

* [ ] Tạo `bronze/player-stat`.
* [ ] Tạo `bronze/invalid`.
* [ ] Tạo `manifests`.
* [ ] Tạo `silver/players`.
* [ ] Tạo `silver/matches`.
* [ ] Tạo `silver/player-match`.
* [ ] Tạo `gold/player-match-features`.
* [ ] Tạo `models`.
* [ ] Tạo `predictions`.
* [ ] Chốt object naming convention.
* [ ] Chốt partitioning convention.
* [ ] Viết tài liệu data lake layout.

---

## Phase 20 — R Project Foundation

* [ ] Khởi tạo R project.
* [ ] Khởi tạo `renv`.
* [ ] Tạo thư mục `R`.
* [ ] Tạo thư mục `scripts`.
* [ ] Tạo config loader.
* [ ] Tạo MinIO storage module.
* [ ] Tạo manifest reader.
* [ ] Thêm Arrow package.
* [ ] Thêm data-processing packages.
* [ ] Thêm model package.
* [ ] Thêm testing package.
* [ ] Tạo Dockerfile.
* [ ] Thêm R Pipeline vào Docker Compose.

---

## Phase 21 — R Preprocessing và Silver

* [ ] List Bronze manifests.
* [ ] Chỉ đọc manifest completed.
* [ ] Đọc Bronze Parquet.
* [ ] Kiểm tra schema.
* [ ] Kiểm tra checksum nếu cần.
* [ ] Chuẩn hóa column types.
* [ ] Xử lý missing values.
* [ ] Loại duplicate toàn cục theo `event_id` giữa các Bronze batches.
* [ ] Tạo bảng players.
* [ ] Tạo bảng matches.
* [ ] Tạo bảng player-match.
* [ ] Ghi Silver Parquet.
* [ ] Ghi processing status.
* [ ] Tạo data-quality summary.
* [ ] Viết test cho preprocessing.

---

## Phase 22 — R Feature Engineering và Gold

* [ ] Chọn feature columns.
* [ ] Tính total distance.
* [ ] Tính headshot ratio.
* [ ] Tính kills per minute.
* [ ] Tính damage per minute.
* [ ] Tính damage per kill.
* [ ] Tính movement per minute.
* [ ] Tính performance versus lobby.
* [ ] Xử lý chia cho 0.
* [ ] Xử lý `NA`.
* [ ] Xử lý giá trị vô hạn.
* [ ] Validate feature types.
* [ ] Tạo feature schema.
* [ ] Tạo feature version.
* [ ] Ghi Gold Parquet.
* [ ] Viết test cho từng feature.

---

## Phase 23 — Exploratory Data Analysis

* [ ] Thống kê số player.
* [ ] Thống kê số match.
* [ ] Thống kê kills.
* [ ] Thống kê damage.
* [ ] Thống kê headshot ratio.
* [ ] Thống kê movement.
* [ ] Vẽ feature distributions.
* [ ] Kiểm tra missing values.
* [ ] Kiểm tra feature correlation.
* [ ] Kiểm tra extreme values.
* [ ] Chọn feature dùng cho model.
* [ ] Xuất EDA report.

---

## Phase 24 — R Model Training

* [ ] Đọc Gold feature dataset.
* [ ] Chọn model features.
* [ ] Validate feature schema.
* [ ] Xử lý missing values.
* [ ] Scale feature nếu cần.
* [ ] Train Isolation Forest.
* [ ] Tạo anomaly score cho training data.
* [ ] Kiểm tra score distribution.
* [ ] Chọn risk thresholds.
* [ ] Lưu `model.rds`.
* [ ] Tạo model version.
* [ ] Tạo model manifest.
* [ ] Lưu feature schema.
* [ ] Lưu training metrics.
* [ ] Upload artifacts lên MinIO.
* [ ] Viết model loading test.

---

## Phase 25 — R Model Scoring

* [ ] Đọc model artifact.
* [ ] Đọc model manifest.
* [ ] Đọc feature schema.
* [ ] Đọc Gold feature dataset.
* [ ] Kiểm tra feature compatibility.
* [ ] Chạy anomaly scoring.
* [ ] Chuẩn hóa score về khoảng 0–1.
* [ ] Gán risk level.
* [ ] Tạo prediction ID.
* [ ] Gắn model version.
* [ ] Gắn feature version.
* [ ] Gắn scoring timestamp.
* [ ] Ghi Predictions Parquet.
* [ ] Upload predictions lên MinIO.
* [ ] Viết scoring test.

---

## Phase 26 — Prediction Evidence

* [ ] Tính feature median.
* [ ] Tính feature percentile.
* [ ] Tính robust z-score.
* [ ] So sánh player với lobby.
* [ ] Xác định feature bất thường nhất.
* [ ] Chọn top evidence features.
* [ ] Ghi player feature value.
* [ ] Ghi reference value.
* [ ] Gán contribution level.
* [ ] Gắn evidence vào prediction.
* [ ] Viết test cho evidence generation.

---

## Phase 27 — Shiny Dashboard Foundation

* [ ] Khởi tạo Shiny application.
* [ ] Tạo configuration.
* [ ] Tạo MinIO connection.
* [ ] Tạo data loader.
* [ ] Đọc Silver data.
* [ ] Đọc Gold data.
* [ ] Đọc predictions.
* [ ] Đọc model manifest.
* [ ] Tạo navigation layout.
* [ ] Tạo error state.
* [ ] Tạo loading state.
* [ ] Tạo Dockerfile.
* [ ] Thêm dashboard vào Docker Compose.

---

## Phase 28 — Shiny Overview

* [ ] Hiển thị tổng số records.
* [ ] Hiển thị tổng số matches.
* [ ] Hiển thị tổng số players.
* [ ] Hiển thị tổng số batches.
* [ ] Hiển thị valid record count.
* [ ] Hiển thị invalid record count.
* [ ] Hiển thị prediction count.
* [ ] Hiển thị high-risk count.
* [ ] Hiển thị model version.
* [ ] Hiển thị feature version.

---

## Phase 29 — Shiny Data Quality

* [ ] Hiển thị missing values.
* [ ] Hiển thị duplicate records.
* [ ] Hiển thị invalid records.
* [ ] Hiển thị invalid reasons.
* [ ] Hiển thị record count theo batch.
* [ ] Hiển thị record distribution.
* [ ] Thêm filter theo source file.
* [ ] Thêm filter theo batch.

---

## Phase 30 — Shiny Player Analysis

* [ ] Tạo player selector.
* [ ] Hiển thị match history.
* [ ] Hiển thị kills.
* [ ] Hiển thị damage.
* [ ] Hiển thị headshot ratio.
* [ ] Hiển thị movement.
* [ ] Hiển thị survival duration.
* [ ] Hiển thị Gold features.
* [ ] So sánh với lobby.
* [ ] Hiển thị risk score.
* [ ] Hiển thị prediction evidence.

---

## Phase 31 — Shiny Risk Analysis

* [ ] Hiển thị prediction table.
* [ ] Sort theo risk score.
* [ ] Filter theo risk level.
* [ ] Filter theo model version.
* [ ] Hiển thị score distribution.
* [ ] Hiển thị số lượng theo risk level.
* [ ] Hiển thị prediction details.
* [ ] Hiển thị top evidence.
* [ ] Hiển thị feature values.
* [ ] Hiển thị reference values.

---

## Phase 32 — End-to-End Integration

* [ ] Chạy Kafka và MinIO.
* [ ] Chạy Dataset Sync.
* [ ] Kiểm tra dataset manifest.
* [ ] Chạy Rust Processor.
* [ ] Chạy Go Replay.
* [ ] Kiểm tra Kafka messages.
* [ ] Kiểm tra Bronze Parquet.
* [ ] Kiểm tra batch manifests.
* [ ] Chạy R Preprocessing.
* [ ] Kiểm tra Silver data.
* [ ] Chạy Feature Engineering.
* [ ] Kiểm tra Gold data.
* [ ] Chạy Model Training.
* [ ] Kiểm tra model artifacts.
* [ ] Chạy Model Scoring.
* [ ] Kiểm tra predictions.
* [ ] Mở Shiny Dashboard.
* [ ] Kiểm tra toàn bộ dashboard.

---

## Phase 33 — Testing

* [ ] Go parser unit tests.
* [ ] Go normalizer unit tests.
* [ ] Go validation unit tests.
* [ ] Go batching unit tests.
* [ ] Go checkpoint unit tests.
* [ ] Go Kafka integration tests.
* [ ] Rust deserialization tests.
* [ ] Rust validation tests.
* [ ] Rust deduplication tests.
* [ ] Rust accumulator tests.
* [ ] Rust Parquet tests.
* [ ] Rust MinIO integration tests.
* [ ] R preprocessing tests.
* [ ] R feature tests.
* [ ] R scoring tests.
* [ ] R evidence tests.
* [ ] End-to-end test với sample dataset.
* [ ] Failure test khi Kafka unavailable.
* [ ] Failure test khi MinIO unavailable.
* [ ] Failure test khi feature schema sai.

---

## Phase 34 — Logging và Error Handling

* [ ] Chuẩn hóa log format.
* [ ] Thêm service name.
* [ ] Thêm operation name.
* [ ] Thêm dataset ID.
* [ ] Thêm replay ID.
* [ ] Thêm batch ID.
* [ ] Thêm Kafka partition.
* [ ] Thêm Kafka offsets.
* [ ] Thêm record count.
* [ ] Thêm duration.
* [ ] Thêm status.
* [ ] Thêm error details.
* [ ] Không log secrets.
* [ ] Xử lý graceful shutdown cho Go.
* [ ] Xử lý graceful shutdown cho Rust.
* [ ] Xử lý command failure cho R.

---

## Phase 35 — Documentation và Demo

* [ ] Hoàn thiện architecture document.
* [ ] Hoàn thiện repository structure document.
* [ ] Viết data contract document.
* [ ] Viết data dictionary.
* [ ] Viết feature dictionary.
* [ ] Viết model description.
* [ ] Viết hướng dẫn cấu hình Kaggle credentials.
* [ ] Viết hướng dẫn chạy Docker Compose.
* [ ] Viết hướng dẫn chạy từng service.
* [ ] Viết hướng dẫn chạy full pipeline.
* [ ] Tạo demo sample dataset.
* [ ] Tạo demo commands.
* [ ] Tạo `make demo`.
* [ ] Chuẩn bị demo script.
* [ ] Chuẩn bị screenshots.
* [ ] Chuẩn bị video demo dự phòng.
* [ ] Viết phần limitations.
* [ ] Viết phần future work.
