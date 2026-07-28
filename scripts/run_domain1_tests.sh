#!/usr/bin/env bash
# =============================================================================
# Domain 1 Test Runner — Fail-Close & Configuration Resilience (TC-01 to TC-06)
# Chạy từng test case bằng docker run riêng lẻ với env override cụ thể
# =============================================================================
set -euo pipefail

# Màu sắc terminal
RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
NC='\033[0m' # No Color

PASS="${GREEN}[PASS]${NC}"
FAIL="${RED}[FAIL]${NC}"

RESULTS=()

run_tc() {
  local tc_id="$1"
  local description="$2"
  local expected_exit="$3"
  local expected_log_pattern="$4"
  shift 4
  local docker_args=("$@")

  echo ""
  echo -e "${YELLOW}▶ Running $tc_id: $description${NC}"

  # Chạy container và capture output + exit code
  set +e
  OUTPUT=$(docker run --rm --network pubg-platform-net "${docker_args[@]}" 2>&1)
  ACTUAL_EXIT=$?
  set -e

  echo "  Exit Code: $ACTUAL_EXIT (expected: $expected_exit)"

  local passed=true

  # Kiểm tra exit code
  if [ "$ACTUAL_EXIT" -ne "$expected_exit" ]; then
    passed=false
    echo "  ✗ Exit code mismatch: got $ACTUAL_EXIT, expected $expected_exit"
  fi

  # Kiểm tra log pattern nếu có
  if [ -n "$expected_log_pattern" ]; then
    if echo "$OUTPUT" | grep -qiE "$expected_log_pattern"; then
      echo "  ✓ Log pattern found: '$expected_log_pattern'"
    else
      passed=false
      echo "  ✗ Log pattern NOT found: '$expected_log_pattern'"
      echo "  Actual output (last 5 lines):"
      echo "$OUTPUT" | tail -5 | sed 's/^/    /'
    fi
  fi

  if $passed; then
    echo -e "  → $PASS $tc_id"
    RESULTS+=("PASS:$tc_id:$description")
  else
    echo -e "  → $FAIL $tc_id"
    RESULTS+=("FAIL:$tc_id:$description")
  fi
}

echo "============================================="
echo "Domain 1 Integration Test Suite - $(date)"
echo "Infra: Kafka=kafka:9092, MinIO=http://minio:9000"
echo "============================================="

# -----------------------------------------------------------------
# TC-01: Thiếu KAFKA_BROKERS trong go-ingestor
# -----------------------------------------------------------------
run_tc "TC-01" "Thiếu KAFKA_BROKERS trong go-ingestor" 1 \
  "biến môi trường chưa khai báo|Fail-Close|KAFKA_BROKERS" \
  -e KAFKA_BROKERS="" \
  -e MINIO_ENDPOINT="http://minio:9000" \
  -e MINIO_ACCESS_KEY="minioadmin" \
  -e MINIO_SECRET_KEY="minioadmin" \
  -e MINIO_BUCKET="fps-anticheat-datalake" \
  -e KAGGLE_DATASET_SLUG="skihikingkevin/pubg-match-deaths" \
  -e KAGGLE_SELECTED_FILE="deaths.csv" \
  -e KAFKA_RAW_TOPIC="pubg.v1.player-stat.raw" \
  -e KAFKA_INVALID_TOPIC="pubg.v1.player-stat.invalid" \
  pubg-go-ingestor:latest

# -----------------------------------------------------------------
# TC-02: Thiếu MINIO_ENDPOINT trong rust-processor
# -----------------------------------------------------------------
run_tc "TC-02" "Thiếu MINIO_ENDPOINT trong rust-processor" 1 \
  "MINIO_ENDPOINT|Fail-Close|Thiếu biến môi trường" \
  -e KAFKA_BROKERS="kafka:9092" \
  -e KAFKA_RAW_TOPIC="pubg.v1.player-stat.raw" \
  -e KAFKA_GROUP_ID="rust-processor-group" \
  -e MINIO_ENDPOINT="" \
  -e MINIO_ACCESS_KEY="minioadmin" \
  -e MINIO_SECRET_KEY="minioadmin" \
  -e MINIO_BUCKET="fps-anticheat-datalake" \
  pubg-rust-processor:latest

# -----------------------------------------------------------------
# TC-03: Sai MinIO S3 Secret Key
# Expected: container exit code 1 sau khi cố gắng kết nối MinIO nhưng bị từ chối
# -----------------------------------------------------------------
run_tc "TC-03" "Sai MINIO_SECRET_KEY (invalid_secret)" 1 \
  "AccessDenied|SignatureDoesNotMatch|S3 error|authentication|forbidden|403|401|invalid|credentials|Forbidden|S3 Storage" \
  -e KAFKA_BROKERS="kafka:9092" \
  -e KAFKA_RAW_TOPIC="pubg.v1.player-stat.raw" \
  -e KAFKA_GROUP_ID="rust-processor-group" \
  -e MINIO_ENDPOINT="http://minio:9000" \
  -e MINIO_ACCESS_KEY="minioadmin" \
  -e MINIO_SECRET_KEY="invalid_secret_key" \
  -e MINIO_BUCKET="fps-anticheat-datalake" \
  pubg-rust-processor:latest

# -----------------------------------------------------------------
# TC-04: Kafka RAW Topic không tồn tại (sai tên topic)
# Expected: consumer lỗi hoặc timeout, exit 1
# -----------------------------------------------------------------
run_tc "TC-04" "KAFKA_RAW_TOPIC trỏ đến topic không tồn tại" 1 \
  "UnknownTopicOrPartition|topic.*not.*exist|unknown.*topic|no such topic|UNKNOWN_TOPIC|không có partition|Topic Existence Check" \
  -e KAFKA_BROKERS="kafka:9092" \
  -e KAFKA_RAW_TOPIC="non_existent_topic_xyz_abc_12345" \
  -e KAFKA_GROUP_ID="rust-processor-group" \
  -e MINIO_ENDPOINT="http://minio:9000" \
  -e MINIO_ACCESS_KEY="minioadmin" \
  -e MINIO_SECRET_KEY="minioadmin" \
  -e MINIO_BUCKET="fps-anticheat-datalake" \
  pubg-rust-processor:latest

# -----------------------------------------------------------------
# TC-05: Lỗi xung đột Unix Domain Socket
# Khởi 2 rust-inference liên tiếp để trigger AddrInUse hoặc unlink
# -----------------------------------------------------------------
# (Tạm thời skip TC-05/TC-05b vì pubg-rust-inference image chưa được build riêng)

# -----------------------------------------------------------------
# TC-06: FLUSH_INTERVAL_MS sai định dạng (không phải số nguyên)
# -----------------------------------------------------------------
run_tc "TC-06" "FLUSH_INTERVAL_MS=abc_invalid (malformed int)" 1 \
  "ParseInt|parse.*error|invalid.*digit|invalid.*config|parse_int|cannot.*parse|không hợp lệ|Fail-Close" \
  -e KAFKA_BROKERS="kafka:9092" \
  -e KAFKA_RAW_TOPIC="pubg.v1.player-stat.raw" \
  -e KAFKA_GROUP_ID="rust-processor-group" \
  -e MINIO_ENDPOINT="http://minio:9000" \
  -e MINIO_ACCESS_KEY="minioadmin" \
  -e MINIO_SECRET_KEY="minioadmin" \
  -e MINIO_BUCKET="fps-anticheat-datalake" \
  -e FLUSH_INTERVAL_MS="abc_invalid" \
  pubg-rust-processor:latest


# =============================================================================
# In kết quả tổng hợp
# =============================================================================
echo ""
echo "============================================="
echo "DOMAIN 1 TEST RESULTS SUMMARY"
echo "============================================="
PASS_COUNT=0
FAIL_COUNT=0
for result in "${RESULTS[@]}"; do
  status="${result%%:*}"
  rest="${result#*:}"
  tc_id="${rest%%:*}"
  desc="${rest#*:}"
  if [ "$status" = "PASS" ]; then
    echo -e "${GREEN}[PASS]${NC} $tc_id — $desc"
    PASS_COUNT=$((PASS_COUNT + 1))
  else
    echo -e "${RED}[FAIL]${NC} $tc_id — $desc"
    FAIL_COUNT=$((FAIL_COUNT + 1))
  fi

done
echo "---------------------------------------------"
echo -e "Total: ${GREEN}$PASS_COUNT PASS${NC} / ${RED}$FAIL_COUNT FAIL${NC}"
echo "============================================="
