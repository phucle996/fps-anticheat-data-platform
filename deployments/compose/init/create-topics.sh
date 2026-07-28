#!/usr/bin/env bash
# ==========================================
# Kafka Topics Initialization Script
# PUBG Anti-Cheat Data Platform
# ==========================================

set -euo pipefail # Thoát script ngay khi có lỗi xảy ra

# Địa chỉ Kafka Broker từ tham số truyền vào hoặc mặc định localhost:9092
KAFKA_BROKER="${1:-kafka:9092}"

echo "[+] Khởi tạo Kafka Topics trên Broker: ${KAFKA_BROKER}..."

# 1. Tạo Topic chứa dữ liệu sự kiện người chơi hợp lệ (Match Stats)
# - Partition: 6 (cho phép phân tán tải và xử lý song song trên nhiều worker)
# - Replication Factor: 1 (phù hợp môi trường local dev single-node)
kafka-topics --bootstrap-server "${KAFKA_BROKER}" \
  --create --if-not-exists \
  --topic "pubg.v1.player-stat.raw" \
  --partitions 6 \
  --replication-factor 1 \
  --config retention.ms=86400000 # Lưu trữ tạm thời 24h (Dữ liệu gốc được S3 Bronze Parquet sao lưu)

echo "  - Topic 'pubg.v1.player-stat.raw' (6 Partitions) đã được tạo thành công."

# 2. Tạo Topic chứa dữ liệu nhật ký hạ gục Telemetry (Kill Events - deaths.csv)
kafka-topics --bootstrap-server "${KAFKA_BROKER}" \
  --create --if-not-exists \
  --topic "pubg.v1.kill-event.raw" \
  --partitions 6 \
  --replication-factor 1 \
  --config retention.ms=86400000 # Lưu trữ tạm thời 24h (Dữ liệu gốc được S3 Bronze Parquet sao lưu)

echo "  - Topic 'pubg.v1.kill-event.raw' (6 Partitions) đã được tạo thành công."

# 3. Tạo Topic Dead-Letter Queue (DLQ) chứa sự kiện không hợp lệ
kafka-topics --bootstrap-server "${KAFKA_BROKER}" \
  --create --if-not-exists \
  --topic "pubg.v1.invalid" \
  --partitions 3 \
  --replication-factor 1 \
  --config retention.ms=2592000000 # Lưu trữ tin nhắn lỗi trong 30 ngày để audit

echo "  - Topic 'pubg.v1.invalid' đã được tạo thành công."

echo "[+] Liệt kê tất cả các Kafka Topics hiện có:"
kafka-topics --bootstrap-server "${KAFKA_BROKER}" --list
