#!/usr/bin/env sh
# ==========================================
# MinIO S3 Buckets Initialization Script
# PUBG Anti-Cheat Data Platform
# ==========================================
# Source of Truth cho MinIO bucket names.
# Tất cả service phải dùng đúng 2 tên bucket này:
#   fps-anticheat-datalake  → Bronze/Silver/Gold Parquet + Manifest
#   pubg-models             → ONNX Model Registry

set -e # Thoát script khi lệnh trả về lỗi

# Cấu hình tham số MinIO
MINIO_URL="${MINIO_URL:-http://minio:9000}"
MINIO_USER="${MINIO_ACCESS_KEY:-minioadmin}"
MINIO_PASS="${MINIO_SECRET_KEY:-minioadmin}"

echo "[+] Khởi tạo kết nối tới MinIO Server tại ${MINIO_URL}..."

# Thiết lập alias cho MinIO Client (mc)
mc alias set localminio "${MINIO_URL}" "${MINIO_USER}" "${MINIO_PASS}"

# ──────────────────────────────────────────────────────────────
# Bucket 1: fps-anticheat-datalake
# Dùng bởi: Rust Processor (Bronze/Manifest), R ETL (Silver/Gold), ML Worker (Gold read)
# ──────────────────────────────────────────────────────────────
DATALAKE_BUCKET="fps-anticheat-datalake"
if ! mc ls "localminio/${DATALAKE_BUCKET}" > /dev/null 2>&1; then
  echo "  - Đang tạo mới bucket '${DATALAKE_BUCKET}'..."
  mc mb "localminio/${DATALAKE_BUCKET}"
else
  echo "  - Bucket '${DATALAKE_BUCKET}' đã tồn tại."
fi

# Bật Bucket Versioning để bảo vệ dữ liệu Parquet khỏi ghi đè
mc version enable "localminio/${DATALAKE_BUCKET}" || true

# ──────────────────────────────────────────────────────────────
# Bucket 2: pubg-models
# Dùng bởi: Python ML Worker (ONNX upload), Rust Inference (ONNX load)
# ──────────────────────────────────────────────────────────────
MODELS_BUCKET="pubg-models"
if ! mc ls "localminio/${MODELS_BUCKET}" > /dev/null 2>&1; then
  echo "  - Đang tạo mới bucket '${MODELS_BUCKET}'..."
  mc mb "localminio/${MODELS_BUCKET}"
else
  echo "  - Bucket '${MODELS_BUCKET}' đã tồn tại."
fi

# Model registry không cần versioning — ONNX bundle được quản lý bằng version prefix path
# vd: pubg-models/pubg-risk/versions/v1/model.onnx

echo "[+] Danh sách Buckets hiện tại trên MinIO:"
mc ls localminio/

echo "[+] Khởi tạo Buckets hoàn tất: ${DATALAKE_BUCKET}, ${MODELS_BUCKET}"
