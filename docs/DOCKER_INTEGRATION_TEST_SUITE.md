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

#### TC-01: Thiếu Biến `KAFKA_BROKERS` trong `go-ingestor`
- **Mục tiêu**: Đảm bảo Ingestor ngắt khẩn cấp khi thiếu cấu hình Kafka Broker.
- **Các bước**:
  1. Khởi chạy `pubg-go-ingestor` với `KAFKA_BROKERS=""`.
- **Kết quả kỳ vọng**: In log `FATAL: phát hiện 1 biến môi trường chưa khai báo: [KAFKA_BROKERS]`, dừng container với **Exit Code 1**.

#### TC-02: Thiếu Biến `MINIO_ENDPOINT` trong `rust-processor`
- **Mục tiêu**: Đảm bảo Engine Rust ngắt ngay lập tức khi thiếu endpoint S3 Storage.
- **Các bước**:
  1. Khởi chạy `pubg-rust-processor` với `MINIO_ENDPOINT=""`.
- **Kết quả kỳ vọng**: Rust App ngắt với lỗi `ConfigError::MissingEnv("MINIO_ENDPOINT")`, Exit Code 1.

#### TC-03: Sai MinIO S3 Access Key / Secret Key
- **Mục tiêu**: Đảm bảo ngắt an toàn khi không xác thực được với MinIO S3.
- **Các bước**:
  1. Đổi `MINIO_SECRET_KEY=invalid_secret` trong docker-compose.
- **Kết quả kỳ vọng**: Báo lỗi `S3 AccessDenied`, không commit Kafka Partition Offset, dừng container.

#### TC-04: Kafka Raw Topic Không Tồn Tại
- **Mục tiêu**: Kiểm tra phản ứng khi Kafka Topic bị xóa hoặc sai tên.
- **Các bước**:
  1. Cấu hình `KAFKA_RAW_TOPIC=non_existent_topic`.
- **Kết quả kỳ vọng**: Consumer không thể subscribe, ném ra `KafkaError::UnknownTopic`, ngắt ứng dụng.

#### TC-05: Lỗi Khóa Unix Domain Socket (`/tmp/rust_inference.sock`) Khi Bị Trùng Port/Path
- **Mục tiêu**: Đảm bảo IPC server xử lý xung đột Socket an toàn.
- **Các bước**:
  1. Khởi chạy 2 tiến trình `rust-inference` ghi đè lên cùng 1 socket path.
- **Kết quả kỳ vọng**: Tiến trình thứ 2 phát hiện socket file đã bận, unlink socket cũ an toàn hoặc ngắt với lỗi `AddrInUse`.

#### TC-06: Chuỗi Cấu Hình Môi Trường Sai Định Định Dạng (Malformed URL/JSON)
- **Mục tiêu**: Kiểm định tính hợp lệ của parser cấu hình.
- **Các bước**:
  1. Truyền `FLUSH_INTERVAL_MS=abc_invalid`.
- **Kết quả kỳ vọng**: Parse failure `ParseIntError`, container ngắt lập tức với Exit Code 1.

---

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
