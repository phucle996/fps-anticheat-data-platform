#!/usr/bin/env bash
# ==========================================
# Kafka Topics Initialization Script
# PUBG Anti-Cheat Data Platform
# ==========================================
# Source of Truth cho Kafka topic names và cấu hình partition.
# Tất cả service phải dùng đúng tên topic được định nghĩa ở đây.
#
# Topic inventory (5 topics):
#   pubg.v1.player-stat.raw      → Raw events từ Go Ingestor (kill + match stats)
#   pubg.v1.kill-event.raw       → Raw kill events riêng (schema match_deaths)
#   pubg.v1.invalid              → Dead-Letter Queue cho events bị lỗi validation
#   pubg.v1.dataset.gold.ready   → Signal R ETL → ML Worker khi Gold Parquet sẵn sàng
#   pubg.v1.ml.model.ready       → Signal ML Worker → Rust Inference khi ONNX model sẵn sàng

set -euo pipefail # Thoát script ngay khi có lỗi xảy ra

# Địa chỉ Kafka Broker từ tham số truyền vào hoặc mặc định kafka:9092
KAFKA_BROKER="${1:-kafka:9092}"

echo "[+] Khởi tạo Kafka Topics trên Broker: ${KAFKA_BROKER}..."

# ──────────────────────────────────────────────────────────────
# 1. Topic: pubg.v1.player-stat.raw
# Dùng bởi: Go Ingestor (produce) → Rust Processor (consume)
# Schema: EventEnvelope với Op=OpMatchSummary hoặc Op=OpKillEvent
# 6 partitions: cho phép xử lý song song tối đa 6 Rust consumer instances
# ──────────────────────────────────────────────────────────────
kafka-topics --bootstrap-server "${KAFKA_BROKER}" \
  --create --if-not-exists \
  --topic "pubg.v1.player-stat.raw" \
  --partitions 6 \
  --replication-factor 1 \
  --config retention.ms=86400000   # Lưu 24h — Bronze Parquet là backup dữ liệu gốc

echo "  - Topic 'pubg.v1.player-stat.raw' (6 partitions) tạo thành công."

# ──────────────────────────────────────────────────────────────
# 2. Topic: pubg.v1.kill-event.raw
# Dùng bởi: Go Ingestor (produce, schema match_deaths) → Rust Processor (consume)
# Tách riêng khỏi player-stat.raw để downstream có thể subscribe theo schema
# ──────────────────────────────────────────────────────────────
kafka-topics --bootstrap-server "${KAFKA_BROKER}" \
  --create --if-not-exists \
  --topic "pubg.v1.kill-event.raw" \
  --partitions 6 \
  --replication-factor 1 \
  --config retention.ms=86400000   # Lưu 24h

echo "  - Topic 'pubg.v1.kill-event.raw' (6 partitions) tạo thành công."

# ──────────────────────────────────────────────────────────────
# 3. Topic: pubg.v1.invalid (Dead-Letter Queue)
# Dùng bởi: Go Ingestor (produce invalid events) → Monitoring / Audit
# Retention 30 ngày để có đủ thời gian điều tra lỗi dữ liệu nguồn
# ──────────────────────────────────────────────────────────────
kafka-topics --bootstrap-server "${KAFKA_BROKER}" \
  --create --if-not-exists \
  --topic "pubg.v1.invalid" \
  --partitions 3 \
  --replication-factor 1 \
  --config retention.ms=2592000000  # Lưu 30 ngày

echo "  - Topic 'pubg.v1.invalid' (3 partitions, DLQ) tạo thành công."

# ──────────────────────────────────────────────────────────────
# 4. Topic: pubg.v1.dataset.gold.ready
# Dùng bởi: R ETL Worker (produce) → Python ML Worker (consume)
# Signal event-driven: 1 message = 1 Gold Parquet batch đã sẵn sàng cho training
# 1 partition đủ vì ML training là sequential (không cần parallel consume)
# ──────────────────────────────────────────────────────────────
kafka-topics --bootstrap-server "${KAFKA_BROKER}" \
  --create --if-not-exists \
  --topic "pubg.v1.dataset.gold.ready" \
  --partitions 1 \
  --replication-factor 1 \
  --config retention.ms=86400000   # Lưu 24h — ML Worker reprocess nếu restart

echo "  - Topic 'pubg.v1.dataset.gold.ready' (1 partition) tạo thành công."

# ──────────────────────────────────────────────────────────────
# 6. Topic: pubg.v1.dataset.bronze.ready
# Dùng bởi: Rust Processor (produce) → R ETL Worker (consume)
# Signal event-driven: 1 message = 1 Bronze Parquet batch đã sẵn sàng cho R ETL
# ──────────────────────────────────────────────────────────────
kafka-topics --bootstrap-server "${KAFKA_BROKER}" \
  --create --if-not-exists \
  --topic "pubg.v1.dataset.bronze.ready" \
  --partitions 1 \
  --replication-factor 1 \
  --config retention.ms=86400000   # Lưu 24h

echo "  - Topic 'pubg.v1.dataset.bronze.ready' (1 partition) tạo thành công."

# ──────────────────────────────────────────────────────────────
# 5. Topic: pubg.v1.ml.model.ready
# Dùng bởi: Python ML Worker (produce) → Rust Inference (consume, reload model)
# Signal event-driven: 1 message = 1 ONNX model version đã upload lên pubg-models bucket
# 1 partition — model reload là critical path, không cần parallelism
# Retention 7 ngày để Rust Inference có thể reprocess sau khi restart
# ──────────────────────────────────────────────────────────────
kafka-topics --bootstrap-server "${KAFKA_BROKER}" \
  --create --if-not-exists \
  --topic "pubg.v1.ml.model.ready" \
  --partitions 1 \
  --replication-factor 1 \
  --config retention.ms=604800000  # Lưu 7 ngày

echo "  - Topic 'pubg.v1.ml.model.ready' (1 partition) tạo thành công."

echo ""
echo "[+] Liệt kê tất cả các Kafka Topics hiện có:"
kafka-topics --bootstrap-server "${KAFKA_BROKER}" --list

echo "[+] Khởi tạo 5 Kafka Topics hoàn tất."
