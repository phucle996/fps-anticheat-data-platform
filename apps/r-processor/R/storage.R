# R Storage Module - Hỗ trợ đọc/ghi dữ liệu Parquet S3/MinIO
# ==============================================================================

suppressPackageStartupMessages({
  if (!requireNamespace("arrow", quietly = TRUE)) {
    install.packages("arrow", repos = "https://cloud.r-project.org")
  }
  library(arrow)
})

read_bronze_parquet <- function(file_path) {
  if (!file.exists(file_path)) {
    stop(sprintf("[ERROR] Không tìm thấy file Parquet tại: %s", file_path))
  }
  
  # Đọc Parquet file siêu tốc bằng Apache Arrow R package
  table_data <- arrow::read_parquet(file_path)
  return(table_data)
}
