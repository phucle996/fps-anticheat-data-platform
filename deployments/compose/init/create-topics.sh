#!/usr/bin/env bash
# ==========================================
# Kafka Topics Initialization Script
# PUBG Anti-Cheat Data Platform
# ==========================================

set -euo pipefail # Thoát script ngay khi có lỗi xảy ra

# Địa chỉ Kafka Broker từ tham số truyền vào hoặc mặc định localhost:9092
KAFKA_BROKER="${1:-kafka:9092}"

echo "[+] Khởi tạo Kafka Topics trên Broker: ${KAFKA_BROKER}..."

# 1. Tạo Topic chứa dữ liệu sự kiện người chơi hợp lệ
# - Partition: 3 (cho phép phân tán tải và xử lý song song trên nhiều worker)
# - Replication Factor: 1 (phù hợp môi trường local dev single-node)
kafka-topics.sh --bootstrap-server "${KAFKA_BROKER}" \
  --create --if-not-exists \
  --topic "pubg.v1.player-stat.raw" \
  --partitions 3 \
  --replication-factor 1 \
  --config retention.ms=604800000 # Lưu trữ tin nhắn trong 7 ngày (7 * 24 * 3600 * 1000 ms)

echo "  - Topic 'pubg.v1.player-stat.raw' đã được tạo hoặc đã tồn tại."

# 2. Tạo Topic Dead-Letter Queue (DLQ) chứa sự kiện không hợp lệ
# - Partition: 1 (dành riêng cho các bản ghi bị lỗi validation / unparseable)
kafka-topics.sh --bootstrap-server "${KAFKA_BROKER}" \
  --create --if-not-exists \
  --topic "pubg.v1.invalid" \
  --partitions 1 \
  --replication-factor 1 \
  --config retention.ms=2592000000 # Lưu trữ tin nhắn lỗi trong 30 ngày để audit

echo "  - Topic 'pubg.v1.invalid' đã được tạo hoặc đã tồn tại."

echo "[+] Liệt kê tất cả các Kafka Topics hiện có:"
kafka-topics.sh --bootstrap-server "${KAFKA_BROKER}" --list
