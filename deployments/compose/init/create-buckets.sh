#!/usr/bin/env sh
# ==========================================
# MinIO S3 Buckets Initialization Script
# PUBG Anti-Cheat Data Platform
# ==========================================

set -e # Thoát script khi lệnh trả về lỗi

# Cấu hình tham số MinIO
MINIO_URL="${MINIO_URL:-http://minio:9000}"       # Endpoint ngầm định trong Docker network
MINIO_USER="${MINIO_ACCESS_KEY:-minioadmin}"     # Access key root
MINIO_PASS="${MINIO_SECRET_KEY:-minioadmin}"     # Secret key root
BUCKET_NAME="${MINIO_BUCKET:-pubg-data}"         # Tên bucket Data Lake chính

echo "[+] Khởi tạo kết nối tới MinIO Server tại ${MINIO_URL}..."

# Thiết lập alias cho MinIO Client (mc)
mc alias set localminio "${MINIO_URL}" "${MINIO_USER}" "${MINIO_PASS}"

# Tạo bucket nếu chưa tồn tại
if ! mc ls "localminio/${BUCKET_NAME}" > /dev/null 2>&1; then
  echo "  - Đang tạo mới bucket '${BUCKET_NAME}'..."
  mc mb "localminio/${BUCKET_NAME}"
else
  echo "  - Bucket '${BUCKET_NAME}' đã tồn tại."
fi

# Thiết lập policy công khai đọc/ghi nội bộ hoặc kiểm tra trạng thái bucket
echo "  - Thiết lập quyền mặc định cho bucket '${BUCKET_NAME}'..."
mc anonymous set download "localminio/${BUCKET_NAME}" || true

echo "[+] Danh sách Buckets hiện tại trên MinIO:"
mc ls localminio/
