# R Manifest Reader Module - Đọc và giải mã file BatchManifest JSON từ S3/Local
# ==============================================================================

suppressPackageStartupMessages({
  if (!requireNamespace("jsonlite", quietly = TRUE)) {
    install.packages("jsonlite", repos = "https://cloud.r-project.org")
  }
  library(jsonlite)
})

read_manifest <- function(manifest_path) {
  if (!file.exists(manifest_path)) {
    stop(sprintf("[ERROR] Không tìm thấy file manifest tại đường dẫn: %s", manifest_path))
  }
  
  manifest_data <- jsonlite::fromJSON(manifest_path)
  
  # Validate các trường bắt buộc trong BatchManifest
  required_fields <- c("batch_id", "source_topic", "data_object_path", "checksum_sha256")
  for (field in required_fields) {
    if (is.null(manifest_data[[field]])) {
      stop(sprintf("[ERROR] File manifest thiếu trường dữ liệu bắt buộc: '%s'", field))
    }
  }
  
  return(manifest_data)
}
