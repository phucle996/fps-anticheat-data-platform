# R Storage Module - Hỗ trợ đọc/ghi dữ liệu Parquet S3/MinIO
# ==============================================================================

read_bronze_parquet <- function(file_path) {
  if (!file.exists(file_path)) {
    stop(sprintf("[ERROR] Không tìm thấy file Parquet tại: %s", file_path))
  }
  
  # Đọc Parquet bằng Arrow package nếu có sẵn, hoặc fallback
  if (requireNamespace("arrow", quietly = TRUE)) {
    table_data <- arrow::read_parquet(file_path)
    return(table_data)
  } else {
    warning("[WARN] R arrow package chưa được nạp, trả về NULL")
    return(NULL)
  }
}
