#!/usr/bin/env bash

# ==============================================================================
# Script Khởi Tạo Tự Động Cấu Trúc Medallion Data Lake Trên MinIO S3
# Dự án: PUBG Anti-Cheat High-Throughput Data Platform
# ==============================================================================

set -euo pipefail

# Biến môi trường mặc định
MINIO_ALIAS="${MINIO_ALIAS:-localminio}"
MINIO_ENDPOINT="${MINIO_ENDPOINT:-http://localhost:9000}"
MINIO_ACCESS_KEY="${MINIO_ACCESS_KEY:-minioadmin}"
MINIO_SECRET_KEY="${MINIO_SECRET_KEY:-minioadmin}"
BUCKET_NAME="${BUCKET_NAME:-fps-anticheat-datalake}"

echo "[INFO] Khởi động tiến trình setup MinIO Data Lake Container..."

# 1. Khởi tạo alias cho mc (MinIO Client) nếu có sẵn mc
if command -v mc &> /dev/null; then
    echo "[INFO] Đang thiết lập kết nối mc alias '${MINIO_ALIAS}' tới ${MINIO_ENDPOINT}..."
    mc alias set "${MINIO_ALIAS}" "${MINIO_ENDPOINT}" "${MINIO_ACCESS_KEY}" "${MINIO_SECRET_KEY}" --api S3v4 || true

    # 2. Tạo bucket nếu chưa tồn tại
    if ! mc ls "${MINIO_ALIAS}/${BUCKET_NAME}" &> /dev/null; then
        echo "[INFO] Đang tạo S3 Bucket '${BUCKET_NAME}'..."
        mc mb "${MINIO_ALIAS}/${BUCKET_NAME}"
    else
        echo "[INFO] S3 Bucket '${BUCKET_NAME}' đã tồn tại."
    fi

    # 3. Tạo cấu trúc thư mục móng 9 tầng Medallion Data Lake
    echo "[INFO] Đang khởi tạo các thư mục móng Medallion Data Lake..."
    mc touch "${MINIO_ALIAS}/${BUCKET_NAME}/bronze/player-stat/.keep"
    mc touch "${MINIO_ALIAS}/${BUCKET_NAME}/bronze/invalid/.keep"
    mc touch "${MINIO_ALIAS}/${BUCKET_NAME}/manifests/.keep"
    mc touch "${MINIO_ALIAS}/${BUCKET_NAME}/silver/players/.keep"
    mc touch "${MINIO_ALIAS}/${BUCKET_NAME}/silver/matches/.keep"
    mc touch "${MINIO_ALIAS}/${BUCKET_NAME}/silver/player-match/.keep"
    mc touch "${MINIO_ALIAS}/${BUCKET_NAME}/gold/player-match-features/.keep"
    mc touch "${MINIO_ALIAS}/${BUCKET_NAME}/models/.keep"
    mc touch "${MINIO_ALIAS}/${BUCKET_NAME}/predictions/.keep"

    echo "[SUCCESS] Đã khởi tạo 100% cấu trúc Medallion Data Lake trên MinIO S3 thành công!"
else
    echo "[WARN] Không tìm thấy 'mc' (MinIO Client) CLI tool. Bỏ qua khởi tạo qua CLI CLI client."
    echo "[INFO] Cấu trúc Data Lake sẽ được khởi tạo tự động khi Rust Stream Processor / R ML Engine khởi chạy."
fi
