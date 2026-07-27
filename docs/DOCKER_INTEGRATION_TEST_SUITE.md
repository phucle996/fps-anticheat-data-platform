# PUBG Anti-Cheat Data Platform — Docker Integration Test Suite

Tài liệu này quy định **Bộ Test Case Kiểm Thử Tích Hợp Toàn Diện trên môi trường Docker Container** (`docs/DOCKER_INTEGRATION_TEST_SUITE.md`) cho hệ thống **FPS Anti-Cheat Data Platform**. 

Bộ test case bao phủ toàn bộ vòng đời ứng dụng từ **Cấu hình biến môi trường Fail-Close**, **Khả năng chịu tải và ngắt mạch Circuit Breaker**, **Validation chất lượng dữ liệu & Dead-Letter Queue (DLQ)**, **Hạ tầng Medallion Data Lake Two-Phase Commit (2PC)**, cho đến **Huấn luyện mô hình Machine Learning, ONNX UDS IPC Engine và REST API Gateway**.

---

## 📐 Tổng Quan Ma Trận Kiểm Thử (Testing Matrix)

| Test Suite | Phạm Vi Kiểm Thử | Thành Phần Liên Quan | Phương Pháp Xác Minh |
| :--- | :--- | :--- | :--- |
| **Suite 1** | Fail-Close & Env Var Resilience | All Container Services | Container Exit Code 1, Log Inspection |
| **Suite 2** | Real-Time Stream & Checkpoint | `go-ingestor`, Kafka, MinIO | Offset tracking, MinIO JSON state |
| **Suite 3** | Data Quality & DLQ Routing | `rust-processor`, MinIO | MinIO S3 `bronze/invalid/` object inspection |
| **Suite 4** | Hysteresis Circuit Breaker & Dynamic Pool | `rust-processor`, Linux `/proc` | CPU/RAM injection, Log Watermarks |
| **Suite 5** | 2PC Medallion Lake & R Feature Engine | `rust-processor`, `r-processor` | Manifest SHA-256 Checksum, Gold Parquet |
| **Suite 6** | ML Training, ONNX Export, IPC UDS & API | `python-ml-worker`, `rust-inference`, `ml-go-api` | UDS Socket Latency < 1ms, API HTTP 200 OK |

---

## 🧪 Detailed Test Cases Specification

### Suite 1: Fail-Close & Environment Configuration Resilience Tests

#### TC-1.1: Thiếu Biến Môi Trường Bắt Buộc (`KAFKA_BROKERS` / `MINIO_ENDPOINT`)
- **Mục tiêu**: Đảm bảo tất cả các container service thực thi nghiêm ngặt nguyên tắc **Fail-Close 100%** (Tự ngắt ứng dụng ngay tức thì, Zero Default Fallback).
- **Các bước thực hiện**:
  1. Khởi chạy container `pubg-go-ingestor` hoặc `pubg-rust-processor` với việc bỏ trống biến `KAFKA_BROKERS=""`.
  2. Quan sát log xuất ra từ container.
- **Kết quả kỳ vọng**:
  - Tiến trình in ra log `FATAL` / `ERROR` thông báo vi phạm biến môi trường.
  - Container dừng lập tức với **Exit Code 1** (không chạy ngầm rác hay dùng default localhost).

#### TC-1.2: Sai Thông Tin Xác Thực MinIO S3 (`MINIO_ACCESS_KEY` / `MINIO_SECRET_KEY`)
- **Mục tiêu**: Đảm bảo khi mất kết nối hoặc sai creds S3 Storage, hệ thống ngắt an toàn mà không làm mất dữ liệu.
- **Các bước thực hiện**:
  1. Cấu hình `MINIO_SECRET_KEY=wrongpassword` trong `docker-compose.yml`.
  2. Khởi chạy `rust-processor`.
- **Kết quả kỳ vọng**:
  - Circuit Breaker ngắt mạch, không commit Kafka Partition Offset.
  - Log ghi nhận lỗi `AccessDenied / SignatureDoesNotMatch` và dừng container an toàn.

---

### Suite 2: Real-Time Data Ingestion & Stream Rate Pacing Tests

#### TC-2.1: Giả Lập Luồng Sự Kiện Rải Rác Thời Gian Thực (`StreamDelayMs`)
- **Mục tiêu**: Kiểm tra khả năng phát luồng dữ liệu rải rác thời gian thực của `go-ingestor`.
- **Các bước thực hiện**:
  1. Kích hoạt `go-ingestor` với cờ `-stream-delay-ms=20`.
  2. Lắng nghe topic Kafka `pubg.v1.player-stat.raw` qua `kafka-console-consumer`.
- **Kết quả kỳ vọng**:
  - Tin nhắn được phát rải rác đều đặn 50 events/giây.
  - Kích thước đệm RAM của `go-ingestor` duy trì ổn định `< 15 MB RAM`.

#### TC-2.2: Tái Lập Điểm Dừng Checkpoint Khi Dừng Container Đột Ngột (Fault Tolerance)
- **Mục tiêu**: Kiểm tra cơ chế khôi phục vị trí nạp dữ liệu (Resume from Checkpoint) khi hạ tầng container bị ngắt điện.
- **Các bước thực hiện**:
  1. Khởi chạy `go-ingestor` nạp dữ liệu tới bản ghi thứ 5,000.
  2. Thực hiện ngắt cưỡng chế container (`docker kill pubg-go-ingestor`).
  3. Khởi động lại container (`docker start pubg-go-ingestor`).
- **Kết quả kỳ vọng**:
  - Container đọc file state `checkpoints/go-replay/state.json` trên MinIO S3.
  - Tự động Resume bắt đầu từ dòng `5,001` mà không bị nạp trùng lặp (Zero Duplicate Re-ingestion).

---

### Suite 3: Data Quality, Semantic Validation & DLQ Routing Tests

#### TC-3.1: Kiểm Định 11 Quy Tắc Data Quality Semantic
- **Mục tiêu**: Xác minh bộ lọc `EventValidator` trong `rust-processor` phân loại chính xác bản ghi hợp lệ và bản ghi rác.
- **Các bước thực hiện**:
  1. Bơm các bản ghi rác chứa vi phạm ngữ nghĩa: `kills = -5`, `win_place_perc = 1.5`, `headshot_kills > kills`.
  2. Kiểm tra đệm xử lý trong `rust-processor`.
- **Kết quả kỳ vọng**:
  - Bản ghi vi phạm bị từ chối chuyển sang luồng nén Parquet chính.
  - Đẩy bản ghi vi phạm dạng JSON vào đệm Dead-Letter Queue (DLQ).

#### TC-3.2: Kiểm Tra Lưu Trữ Vùng Đệm Vi Phạm MinIO S3 (`bronze/invalid/`)
- **Mục tiêu**: Đảm bảo các bản ghi lỗi được bảo toàn nguyên trạng trên S3 để phục vụ phân tích lỗi (Audit & Root Cause Analysis).
- **Các bước thực hiện**:
  1. Kiểm tra bucket `fps-anticheat-datalake` trên MinIO.
- **Kết quả kỳ vọng**:
  - Xuất hiện file dữ liệu vi phạm tại đường dẫn Hive Partition: `bronze/invalid/year=YYYY/month=MM/day=DD/invalid_records_*.json`.

---

### Suite 4: Dynamic Worker Allocation Pool & Hysteresis Circuit Breaker Tests

#### TC-4.1: Kiểm Thử Chịu Tải Đột Biến (Spike Load Backpressure)
- **Mục tiêu**: Kiểm tra khả năng tự động điều phối worker bất đồng bộ của `RDynamicWorkerPool`.
- **Các bước thực hiện**:
  1. Bơm dồn dập 50,000 bản ghi thô vào Kafka trong 1 giây.
  2. Theo dõi nhịp xử lý và log của `rust-processor`.
- **Kết quả kỳ vọng**:
  - Thread pool tự động spawn song song các Worker Tasks giải tỏa đệm RAM.
  - End-to-End Latency duy trì `< 5ms`.

#### TC-4.2: Ngắt Mạch Hysteresis Circuit Breaker Khi Quá Tải CPU/RAM (`/proc` Watermarks)
- **Mục tiêu**: Kiểm tra bộ ngắt mạch bảo vệ an toàn hệ thống `ResourceCircuitBreaker`.
- **Các bước thực hiện**:
  1. Giả lập ép tải CPU $\ge$ 80% hoặc RAM $\ge$ 85% trong container `rust-processor`.
  2. Kiểm tra trạng thái Circuit Breaker.
  3. Giảm tải tài nguyên xuống CPU $\le$ 75% và RAM $\le$ 80%.
- **Kết quả kỳ vọng**:
  - Khi vượt trần `80%/85%`, trạng thái ngắt sang `OPEN`, tạm dừng spawn R Worker mới.
  - Khi hạ xuống `75%/80%` (Hysteresis Gap), trạng thái phục hồi về `CLOSED`, tiếp tục xử lý trơn tru.

---

### Suite 5: Medallion Data Lake 2PC & R Feature Preprocessing Tests

#### TC-5.1: Kiểm Tra Nguyên Tắc Durable Two-Phase Commit (2PC)
- **Mục tiêu**: Đảm bảo Kafka Offset chỉ được commit khi cả 2 phase ghi đĩa S3 đều thành công 100%.
- **Các bước thực hiện**:
  1. Kiểm tra luồng ghi Bronze Parquet (Phase 1) và Manifest JSON (Phase 2).
- **Kết quả kỳ vọng**:
  - Phase 1 (Data Parquet) & Phase 2 (Audit Manifest JSON) thành công 100%.
  - Kafka Partition Offset chỉ commit SAU khi Phase 1 & 2 hoàn tất (Zero Data Loss 100%).

#### TC-5.2: Khởi Chạy R Feature Engine & Nạp Tầng Gold Matrix
- **Mục tiêu**: Kiểm tra khả năng thực thi của `r-processor` trên môi trường Linux container.
- **Các bước thực hiện**:
  1. Đọc tín hiệu Batch Manifest từ MinIO S3.
  2. Thực thi `silver_preprocessor.R` và `gold_feature_engine.R`.
- **Kết quả kỳ vọng**:
  - Dữ liệu Silver Entities và Gold Feature Matrix được trích xuất thành công.
  - Đóng gói Parquet ghi vào `gold/features/year=YYYY/...` trên MinIO S3.

---

### Suite 6: Machine Learning Training, ONNX Export, IPC UDS & API Gateway Tests

#### TC-6.1: Huấn Luyện Mô Hình LightGBM & Xuất Artifacts ONNX
- **Mục tiêu**: Kiểm tra luồng tự động huấn luyện mô hình ML của `python-ml-worker`.
- **Các bước thực hiện**:
  1. Đọc dữ liệu Gold Feature Matrix từ MinIO.
  2. Khởi chạy `trainer.py` và `onnx_exporter.py`.
- **Kết quả kỳ vọng**:
  - Mô hình LightGBM huấn luyện đạt độ chính xác mong muốn.
  - File `model.onnx` và `model_metadata.json` được upload thành công lên MinIO bucket `pubg-models`.

#### TC-6.2: Kiểm Thử Động Cơ Suy Luận Rust ONNX Engine Qua Unix Domain Socket (UDS IPC)
- **Mục tiêu**: Kiểm thử tốc độ suy luận gian lận thời gian thực qua giao tiếp Socket IPC.
- **Các bước thực hiện**:
  1. Khởi chạy `rust-inference` server tạo UDS socket `/tmp/rust_inference.sock`.
  2. Truyền vector đặc trưng player stat qua socket.
- **Kết quả kỳ vọng**:
  - Nhận phản hồi kết quả dự đoán (Risk Score & Classification) với **Latency < 1ms**.

#### TC-6.3: Kiểm Thử Toàn Diện Go REST API Gateway & Streamlit Dashboard Integration
- **Mục tiêu**: Kiểm tra tính chính xác của dữ liệu phục vụ báo cáo và giao diện người dùng.
- **Các bước thực hiện**:
  1. Gửi HTTP Request tới `ml-go-api` gateway (`GET /api/v1/health`, `GET /api/v1/players/{id}`).
  2. Kiểm tra truy vấn trên 4 trang Streamlit Dashboard.
- **Kết quả kỳ vọng**:
  - Tất cả các endpoint trả về `200 OK` JSON phản hồi chính xác.
  - Giao diện Streamlit render mượt mà, không gặp lỗi Null Pointer hay ném Exception.

---

## 🛠️ Hướng Dẫn Thực Thi Bộ Integration Test Cases Trên Docker

### 1. Khởi chạy hạ tầng container tập trung:
```bash
docker compose -f deployments/compose/docker-compose.yml up -d
```

### 2. Kiểm tra trạng thái sức khỏe (Health Check) của các container services:
```bash
docker compose -f deployments/compose/docker-compose.yml ps
```

### 3. Theo dõi log thời gian thực kiểm tra toàn bộ luồng pipeline end-to-end:
```bash
docker compose -f deployments/compose/docker-compose.yml logs -f
```
