# PUBG Anti-Cheat Data Platform — Exhaustive Docker Integration Test Suite

Tài liệu này quy định **Bộ 42 Test Cases Kiểm Thử Tích Hợp Toàn Diện trên môi trường Docker Container** (`docs/DOCKER_INTEGRATION_TEST_SUITE.md`) cho hệ thống **FPS Anti-Cheat Data Platform**.

Bộ test cases này được thiết kế theo các tiêu chuẩn khắt khe nhất của **Cloud-Native & High Availability Systems**, bao phủ 100% các kịch bản thực tế: **Cấu hình Fail-Close**, **11 Quy tắc Semantic Data Quality**, **Tự động khôi phục Checkpoint**, **Ngắt mạch Hysteresis Circuit Breaker**, **Durable Two-Phase Commit (2PC)**, **Khả năng chịu tải bão tin**, **Huấn luyện mô hình ML & Suy luận ONNX UDS IPC < 1ms**, cho tới **REST API Gateway và Giao diện Streamlit Dashboard**.

---

## 📐 Ma Trận 42 Test Cases Theo 8 Miền Kiểm Thử (Testing Domains Matrix)

| Miền Kiểm Thử (Domain) | Mã Test Cases | Số Lượng Cases | Phạm Vi Xử Lý |
| :--- | :--- | :--- | :--- |
| **Domain 1: Fail-Close & Config Resilience** | `TC-01` $\rightarrow$ `TC-06` | 6 Test Cases | Tất cả các Docker Container Services |
| **Domain 2: Data Quality & 11 Semantic Rules** | `TC-07` $\rightarrow$ `TC-17` | 11 Test Cases | `rust-processor`, MinIO DLQ |
| **Domain 3: Stream Engine & Checkpointing** | `TC-18` $\rightarrow$ `TC-22` | 5 Test Cases | `go-ingestor`, Kafka, MinIO |
| **Domain 4: Backpressure & Circuit Breaker** | `TC-23` $\rightarrow$ `TC-26` | 4 Test Cases | `rust-processor`, Linux `/proc` |
| **Domain 5: Durable 2PC & Hive Partitioning** | `TC-27` $\rightarrow$ `TC-30` | 4 Test Cases | `rust-processor`, MinIO S3 Bronze |
| **Domain 6: R Feature Engine & Warm Daemon** | `TC-31` $\rightarrow$ `TC-34` | 4 Test Cases | `r-processor`, Silver & Gold Parquet |
| **Domain 7: ML Training, ONNX Export & IPC UDS** | `TC-35` $\rightarrow$ `TC-38` | 4 Test Cases | `python-ml-worker`, `rust-inference` |
| **Domain 8: REST API Gateway & Streamlit UI** | `TC-39` $\rightarrow$ `TC-42` | 4 Test Cases | `ml-go-api`, `streamlit-dashboard` |

---

## 🧪 Chi Tiết 42 Test Cases Specification

### 🔴 Domain 1: Fail-Close & Configuration Resilience (TC-01 -> TC-06)

> **Môi trường thực thi**: Docker `pubg-platform-net`, Kafka `confluentinc/cp-kafka:7.6.1` (healthy), MinIO `minio/minio:RELEASE.2024-05-28T17-19-04Z` (healthy).
> **Thời gian chạy**: 2026-07-28T07:24 +07:00 (re-run sau khi fix)
> **Images**: `pubg-go-ingestor:latest` (Go 1.23), `pubg-rust-processor:latest` (Rust 1.86 — rebuilt với fixes)

#### [PASS] TC-01: Thiếu Biến `KAFKA_BROKERS` trong `go-ingestor`
- **Mục tiêu**: Đảm bảo Ingestor ngắt khẩn cấp khi thiếu cấu hình Kafka Broker.
- **Command**:
  ```bash
  docker run --rm --network pubg-platform-net -e KAFKA_BROKERS="" ... pubg-go-ingestor:latest
  ```
- **Actual Output**:
  ```json
  {"level":"info","msg":"Khởi tạo tiến trình dataset-sync entrypoint...","service":"go-ingestor"}
  {"error":"cấu hình thất bại (Fail-Close Active): phát hiện 1 biến môi trường chưa khai báo: [KAFKA_BROKERS] (Fail-Close Rule Violated)","level":"fatal","msg":"Nạp cấu hình ứng dụng thất bại"}
  ```
- **Exit Code thực tế**: `1` | **Kết quả**: ✅ **PASS** — Fail-Close Active, log FATAL đúng spec.

---

#### [PASS] TC-02: Thiếu / Rỗng Biến `MINIO_ENDPOINT` trong `rust-processor`
- **Mục tiêu**: Đảm bảo ngắt ngay lập tức khi thiếu endpoint S3 Storage.
- **Fix đã áp dụng**: `config.rs` — `get_required_env()` giờ reject cả giá trị rỗng sau trim. Validate URL phải bắt đầu `http://` hoặc `https://`.
- **Command**:
  ```bash
  docker run --rm --network pubg-platform-net -e MINIO_ENDPOINT="" ... pubg-rust-processor:latest
  ```
- **Actual Output**:
  ```json
  {"level":"INFO","message":"Khởi động tiến trình Rust Stream Processor Engine (Thin Entrypoint Active)..."}
  {"level":"WARN","message":"Dừng chương trình do lỗi nạp cấu hình (Fail-Close Triggered): Lỗi cấu hình: Biến môi trường bắt buộc 'MINIO_ENDPOINT' không được để trống (Fail-Close Triggered)"}
  Error: Config("Biến môi trường bắt buộc 'MINIO_ENDPOINT' không được để trống (Fail-Close Triggered)")
  ```
- **Exit Code thực tế**: `1` | **Kết quả**: ✅ **PASS** — Clean Fail-Close, log rõ tên biến vi phạm.

---

#### [PASS] TC-03: Sai MinIO S3 Secret Key trong `rust-processor`
- **Mục tiêu**: Đảm bảo ngắt an toàn trước khi consume khi sai creds S3.
- **Fix đã áp dụng**: `storage/minio.rs` + `app.rs` — Thêm `preflight_check()` chạy HEAD request vào MinIO S3 tại startup. Nếu `403 Forbidden` → Fail-Close ngay trước consume loop.
- **Command**:
  ```bash
  docker run --rm --network pubg-platform-net -e MINIO_SECRET_KEY="invalid_secret_key_xyz" ... pubg-rust-processor:latest
  ```
- **Actual Output**:
  ```json
  {"level":"INFO","message":"Thực thi S3 Pre-flight Connectivity Check (TC-03 Fail-Close Guard)..."}
  {"level":"ERROR","message":"S3 Pre-flight Check thất bại — Dừng chương trình trước khi consume Kafka (Fail-Close Triggered)","error":"Lỗi S3 Storage: S3 Pre-flight Check thất bại — Không thể kết nối hoặc xác thực MinIO S3: Generic S3 error: Client error with status 403 Forbidden: No Body (Fail-Close Triggered)"}
  Error: Storage("S3 Pre-flight Check thất bại — ... 403 Forbidden ...")
  ```
- **Exit Code thực tế**: `1` | **Kết quả**: ✅ **PASS** — `403 Forbidden` detected tại startup, Fail-Close trước consume.

---

#### [PASS] TC-04: `KAFKA_RAW_TOPIC=non_existent_topic_xyz_abc_12345` trong `rust-processor`
- **Mục tiêu**: Ngắt ứng dụng khi topic không tồn tại — không được ngồi chờ vô hạn.
- **Fix đã áp dụng**: `ingest/consumer.rs` + `app.rs` — Thêm `verify_topic_exists()` dùng `fetch_metadata()` với timeout 10s. Topic không có partition → Fail-Close.
- **Command**:
  ```bash
  docker run --rm --network pubg-platform-net -e KAFKA_RAW_TOPIC="non_existent_topic_xyz_abc_12345" ... pubg-rust-processor:latest
  ```
- **Actual Output**:
  ```json
  {"level":"INFO","message":"S3 Pre-flight Check thành công — MinIO S3 kết nối và xác thực OK"}
  {"level":"INFO","message":"Thực thi Kafka Topic Existence Check (TC-04 Fail-Close Guard)..."}
  {"level":"ERROR","message":"Kafka Topic Existence Check thất bại — Dừng chương trình (Fail-Close Triggered)","error":"Lỗi Kafka: Topic 'non_existent_topic_xyz_abc_12345' tồn tại nhưng không có partition nào (Fail-Close Triggered)"}
  Error: Kafka("Topic 'non_existent_topic_xyz_abc_12345' tồn tại nhưng không có partition nào (Fail-Close Triggered)")
  ```
- **Exit Code thực tế**: `1` | **Kết quả**: ✅ **PASS** — Topic existence verified tại startup, Fail-Close ngay.

---

#### [SKIP] TC-05: Xung Đột Unix Domain Socket `/tmp/rust_inference.sock`
- **Mục tiêu**: Đảm bảo IPC server xử lý xung đột Socket an toàn.
- **Kết quả**: ⏭️ **SKIP** — `pubg-rust-inference:latest` chưa có Dockerfile riêng trong repo. Service inference được nhúng chung vào `rust-processor`. Cần tách thành service độc lập trước khi test.

---

#### [PASS] TC-06: `FLUSH_INTERVAL_MS=abc_invalid` (Malformed Config) trong `rust-processor`
- **Mục tiêu**: Container ngắt ngay khi nhận giá trị không phải số nguyên.
- **Fix đã áp dụng**: `config.rs` — `parse_optional_u64()` / `parse_optional_usize()` parse explicit, ném `AppError::Config` với tên biến + giá trị vi phạm nếu sai định dạng.
- **Command**:
  ```bash
  docker run --rm --network pubg-platform-net -e FLUSH_INTERVAL_MS="abc_invalid" ... pubg-rust-processor:latest
  ```
- **Actual Output**:
  ```json
  {"level":"INFO","message":"Khởi động tiến trình Rust Stream Processor Engine (Thin Entrypoint Active)..."}
  {"level":"WARN","message":"Dừng chương trình do lỗi nạp cấu hình (Fail-Close Triggered): Lỗi cấu hình: Biến 'FLUSH_INTERVAL_MS' có giá trị 'abc_invalid' không hợp lệ — phải là số nguyên dương (Fail-Close Triggered)"}
  Error: Config("Biến 'FLUSH_INTERVAL_MS' có giá trị 'abc_invalid' không hợp lệ — phải là số nguyên dương (Fail-Close Triggered)")
  ```
- **Exit Code thực tế**: `1` | **Kết quả**: ✅ **PASS** — Log chính xác tên biến + giá trị vi phạm, Fail-Close ngay.

---

> **✅ Domain 1 Final Summary**: **5/6 PASS, 0/6 FAIL, 1/6 SKIP**
>
> | TC | Status | Fix Applied |
> |---|---|---|
> | TC-01 `go-ingestor KAFKA_BROKERS=""` | ✅ PASS | Pre-existing |
> | TC-02 `rust-processor MINIO_ENDPOINT=""` | ✅ PASS | `get_required_env()` reject empty + URL prefix validate |
> | TC-03 `rust-processor wrong S3 creds` | ✅ PASS | `MinioWriter::preflight_check()` HEAD probe → 403 Fail-Close |
> | TC-04 `rust-processor wrong topic` | ✅ PASS | `KafkaConsumer::verify_topic_exists()` fetch_metadata → Fail-Close |
> | TC-05 `rust-inference UDS conflict` | ⏭️ SKIP | Service chưa tồn tại độc lập |
> | TC-06 `rust-processor FLUSH_INTERVAL_MS=abc` | ✅ PASS | `parse_optional_u64()` explicit fail-close |


### 🟡 Domain 2: Data Quality & 11 Semantic Boundary Checks (TC-07 -> TC-17)

#### TC-07: Vi Phạm Âm Số Lượng Kills (`kills < 0`)
- **Các bước**: Bơm event `kills = -3`.
- **Kết quả kỳ vọng**: `EventValidator` loại bỏ event, ghi log `SemanticViolation("kills_negative")`.

#### TC-08: Vi Phạm Tỷ Lệ Xếp Hạng Vượt Ngưỡng (`win_place_perc > 1.0` hoặc `< 0.0`)
- **Các bước**: Bơm event `win_place_perc = 1.25`.
- **Kết quả kỳ vọng**: Bị lọc bỏ, không nén vào file Bronze Parquet chính.

#### TC-09: Vi Phạm Tỷ Lệ Bắn Vào Đầu Không Hợp Lý (`headshot_kills > kills`)
- **Các bước**: Bơm event `headshot_kills = 10` nhưng `kills = 2`.
- **Kết quả kỳ vọng**: Lọc bỏ với nguyên nhân `InvalidMetricCombination("headshots_exceed_kills")`.

#### TC-10: Vi Phạm Tốc Độ Di Chuyển Bất Thường (`walk_distance / duration > MAX_SPEED`)
- **Các bước**: Bơm event di chuyển `50,000m` trong `10 giây`.
- **Kết quả kỳ vọng**: Đánh dấu vi phạm tốc độ di chuyển hack speed.

#### TC-11: Rỗng Mã Định Danh Người Chơi (`player_id_hash == ""`)
- **Các bước**: Bơm event rỗng `player_id_hash`.
- **Kết quả kỳ vọng**: Bị từ chối do thiếu khóa chính định danh.

#### TC-12: Rỗng Mã Trận Đấu (`match_id == ""`)
- **Các bước**: Bơm event thiếu `match_id`.
- **Kết quả kỳ vọng**: Vi phạm ràng buộc liên kết trận đấu.

#### TC-13: Vi Phạm Âm Sát Thương Gây Ra (`damage_dealt < 0`)
- **Các bước**: Bơm event `damage_dealt = -150.0`.
- **Kết quả kỳ vọng**: Loại bỏ do sát thương không thể âm.

#### TC-14: Thứ Tự Thời Gian Sự Kiện Đến Quá Trễ (Late-Arriving Events > 24 Hours)
- **Các bước**: Bơm event có timestamp lùi về 2 ngày trước.
- **Kết quả kỳ vọng**: Đưa vào vùng xử lý event trễ hoặc từ chối nạp Bronze.

#### TC-15: Chuỗi Payload JSON Sai Syntax Schema
- **Các bước**: Bơm chuỗi JSON lỗi cấu trúc `{"event_id": 123...`.
- **Kết quả kỳ vọng**: Lỗi deserialization, đẩy nguyên bản rác vào DLQ.

#### TC-16: Trùng Mã Sự Kiện Trong Cùng Batch (`event_id` Hash Collision)
- **Các bước**: Bơm 2 bản ghi trùng hệt `event_id` trong cùng 1 Batch.
- **Kết quả kỳ vọng**: `EventDeduplicator` loại bỏ 1 bản ghi trùng, duy trì đúng 1 bản ghi duy nhất.

#### TC-17: Kiểm Tra Lưu Trữ Vùng Đệm Vi Phạm MinIO S3 DLQ (`bronze/invalid/`)
- **Các bước**: Đọc dữ liệu từ bucket `fps-anticheat-datalake`.
- **Kết quả kỳ vọng**: Xuất hiện file vi phạm tại `bronze/invalid/year=YYYY/month=MM/day=DD/invalid_records_*.json`.

---

### 🟢 Domain 3: Real-Time Stream Engine & Rate Limiting (TC-18 -> TC-22)

#### TC-18: Phát Sự Kiện Rải Rác 50 Events/Sec (`-stream-delay-ms=20`)
- **Kết quả kỳ vọng**: Dữ liệu nạp đều đặn vào Kafka 50 bản ghi/giây, không bị dồn tụ.

#### TC-19: Chịu Tải Bão Dữ Liệu Tốc Độ Cao (50,000 Events/Sec Stress Burst)
- **Kết quả kỳ vọng**: Kafka & Rust Ingestor xử lý trơn tru, không cạn bộ nhớ RAM.

#### TC-20: Kiểm Kiểm Trần Bộ Nhớ RAM `go-ingestor` (< 15MB RAM Ceiling)
- **Kết quả kỳ vọng**: Đọc streaming `bufio.Scanner` duy trì RAM cố định `< 15 MB` dù nạp file 670MB.

#### TC-21: Ngắt Tín Hiệu An Toàn Của OS (`SIGINT` / `SIGTERM` Graceful Shutdown)
- **Kết quả kỳ vọng**: `go-ingestor` flush hết đệm dở dang và lưu checkpoint trước khi thoát.

#### TC-22: Khôi Phục Vị Trí Nạp Khi Dừng Cưỡng Chế Container (`docker kill`)
- **Kết quả kỳ vọng**: Khởi động lại container tự động đọc `checkpoints/go-replay/state.json` và resume chính xác dòng tiếp theo.

---

### 🔵 Domain 4: Backpressure, Dynamic Pool & Hysteresis Circuit Breaker (TC-23 -> TC-26)

#### TC-23: Ngắt Mạch Khi Vượt Trần CPU (`CPU >= 80%` -> OPEN)
- **Kết quả kỳ vọng**: Circuit Breaker chuyển trạng thái `OPEN`, tạm ngắt dispatch R Worker mới.

#### TC-24: Ngắt Mạch Khi Vượt Trần RAM (`RAM >= 85%` -> OPEN)
- **Kết quả kỳ vọng**: Tạm dừng spawn worker để bảo vệ bộ nhớ hệ thống khỏi bị crash.

#### TC-25: Phục Hồi Nhịp Ngắt Khi Hạ Tải Hysteresis Gap (`CPU <= 75%` AND `RAM <= 80%` -> CLOSED)
- **Kết quả kỳ vọng**: Trạng thái phục hồi về `CLOSED`, tiếp tục spawn R Worker bình thường.

#### TC-26: Thử Nghiệm Chống Dao Động Đóng/Mở Liên Tục (Anti-Flapping Stability)
- **Kết quả kỳ vọng**: Khoảng trễ Hysteresis Gap 5% triệt tiêu hoàn toàn hiện tượng Flapping đóng mở liên tục.

---

### 🟣 Domain 5: Durable 2PC Data Lake & Hive Partitioning (TC-27 -> TC-30)

#### TC-27: Phase 1 Ghi Parquet Data File Thành Công
- **Kết quả kỳ vọng**: File Parquet nén Zstandard được ghi thành công lên S3 `bronze/player-stat/...`.

#### TC-28: Phase 2 Ghi Audit Manifest JSON & Khớp Checksum SHA-256
- **Kết quả kỳ vọng**: File Manifest JSON chứa chính xác SHA-256 checksum của file Parquet ở Phase 1.

#### TC-29: Cam Kết Đảm Bảo Commit Kafka Offset Nguyên Tử (Atomic Commitment)
- **Kết quả kỳ vọng**: Kafka Offset chỉ được commit SAU khi Phase 1 và Phase 2 đều trả về thành công 100%.

#### TC-30: Kiểm Tra Cấu Trúc Hive Partitioning Đường Dẫn MinIO
- **Kết quả kỳ vọng**: Đường dẫn tuân thủ chuẩn `bronze/player-stat/year=YYYY/month=MM/day=DD/data_*.parquet`.

---

### 🟠 Domain 6: R Feature Preprocessing & Warm Daemon Pool (TC-31 -> TC-34)

#### TC-31: Thực Thi Tiến Trình Subprocess R Cách Ly Hoàn Toàn
- **Kết quả kỳ vọng**: Lỗi trong script R không làm ảnh hưởng hay sụp đổ tiến trình nạp Rust Processor chính.

#### TC-32: Làm Sạch Dữ Liệu Tầng Silver Entities (`silver_preprocessor.R`)
- **Kết quả kỳ vọng**: Xuất ra Silver entities Parquet sạch 100% null/rác.

#### TC-33: Trích Xuất Ma Trận Đặc Trưng Gold Matrix (`gold_feature_engine.R`)
- **Kết quả kỳ vọng**: Ghi ma trận Gold features vào S3 `gold/features/year=YYYY/...`.

#### TC-34: Tự Động Thu Hồi 100% RAM Khi Hết 5 Giây Idle (`5s Idle Timeout Exit 0`)
- **Kết quả kỳ vọng**: Sau 5 giây ngưng có batch mới, `daemon_worker.R` thoát (`Exit 0`), Linux Kernel thu hồi 100% RAM.

---

### 🟤 Domain 7: ML Model Training, ONNX Export & IPC UDS Engine (TC-35 -> TC-38)

#### TC-35: Tự Động Huấn Luyện Mô Hình LightGBM Từ Dữ Liệu Gold Matrix
- **Kết quả kỳ vọng**: Worker Python đọc dữ liệu Gold, huấn luyện thành công mô hình phân loại gian lận.

#### TC-36: Export Mô Hình Sang Chuẩn ONNX & Upload MinIO (`pubg-models/model.onnx`)
- **Kết quả kỳ vọng**: File `model.onnx` và `model_metadata.json` được upload thành công lên MinIO.

#### TC-37: Đo Đạc Độ Trễ Suy Luận Qua Unix Domain Socket (`/tmp/rust_inference.sock`)
- **Kết quả kỳ vọng**: Động cơ Rust ONNX phản hồi kết quả dự đoán với **Latency < 1ms**.

#### TC-38: Bảo Vệ An Toàn Khi Cập Nhật Mô Hình Mới (Hot-Reload Fallback Safety)
- **Kết quả kỳ vọng**: Khi upload phiên bản mô hình mới, UDS Engine tự động nạp lại mà không bị sụp socket.

---

### ⚪ Domain 8: REST API Gateway & Streamlit UI Integration (TC-39 -> TC-42)

#### TC-39: Kiểm Kiểm HTTP Phản Hồi Từ API Gateway (`GET /api/v1/health`, `GET /api/v1/players/{id}`)
- **Kết quả kỳ vọng**: Trả về `200 OK` JSON đúng cấu trúc schema.

#### TC-40: Kiểm Thử Chịu Tải Đồng Thời REST API (Concurrent HTTP Requests)
- **Kết quả kỳ vọng**: Phục vụ hàng trăm request đồng thời không bị lỗi Null Pointer hay 500 Internal Error.

#### TC-41: Hiển Thị Trang Overview & Preprocessing Before vs After Trên Streamlit
- **Kết quả kỳ vọng**: Biểu đồ so sánh 1-1 trước và sau khi làm sạch dữ liệu hiển thị chính xác.

#### TC-42: Hiển Thị Trang Player Analysis & Risk Score Evidence
- **Kết quả kỳ vọng**: Render chính xác chỉ số người chơi, risk score và top evidence features mà không gặp exception.

---

## 🛠️ Hướng Dẫn Kích Hoạt Bộ 42 Integration Test Cases Trên Docker

### 1. Khởi chạy toàn bộ hạ tầng Container Services:
```bash
docker compose -f deployments/compose/docker-compose.yml up -d
```

### 2. Kiểm tra trạng thái sức khỏe toàn bộ Containers:
```bash
docker compose -f deployments/compose/docker-compose.yml ps
```

### 3. Xem log thời gian thực kiểm tra kết quả thực thi:
```bash
docker compose -f deployments/compose/docker-compose.yml logs -f
```
