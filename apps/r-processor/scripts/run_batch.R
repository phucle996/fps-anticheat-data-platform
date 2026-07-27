#!/usr/bin/env Rscript

# ==============================================================================
# Rscript CLI Entrypoint - Đón nhận tham số `--manifest <path>` từ Rust Worker
# ==============================================================================

# Parse tham số CLI
args <- commandArgs(trailingOnly = TRUE)
manifest_path <- NULL

for (i in seq_along(args)) {
  if (args[i] == "--manifest" && i < length(args)) {
    manifest_path <- args[i + 1]
  }
}

if (is.null(manifest_path)) {
  cat("[ERROR] Bắt buộc truyền tham số `--manifest <path>` cho Rscript!\n")
  quit(status = 1)
}

cat(sprintf("[INFO] Rscript CLI nhận yêu cầu xử lý Manifest: %s\n", manifest_path))

# Nạp các module R cốt lõi
source("R/config.R")
source("R/manifest_reader.R")
source("R/storage.R")
source("R/silver_preprocessor.R")

tryCatch({
  # 1. Đọc và kiểm tra Manifest JSON
  manifest <- read_manifest(manifest_path)
  cat(sprintf("[SUCCESS] Đã đọc thành công Batch ID '%s' (Total Records: %d)\n", 
              manifest$batch_id, manifest$total_records_read))
  
  # 2. Xử lý tiền xử lý và trích xuất 3 bảng Silver Layer Entities
  report <- process_silver_entities(manifest_path)
  
  cat(sprintf("[SUCCESS] Hoàn tất tiến trình R Preprocessor cho Batch '%s' (Unique: %d, Players: %d, Matches: %d)\n",
              report$batch_id, report$unique_records, report$players_count, report$matches_count))
  quit(status = 0)
}, error = function(e) {
  cat(sprintf("[ERROR] Rscript Subprocess thất bại: %s\n", e$message))
  quit(status = 1)
})
