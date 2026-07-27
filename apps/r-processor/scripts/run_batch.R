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

tryCatch({
  # 1. Đọc và kiểm tra Manifest JSON
  manifest <- read_manifest(manifest_path)
  cat(sprintf("[SUCCESS] Đã đọc thành công Batch ID '%s' (Total Records: %d)\n", 
              manifest$batch_id, manifest$total_records_read))
  
  cat("[INFO] Hoàn tất tiến trình R Analytics Worker cho Batch thành công.\n")
  quit(status = 0)
}, error = function(e) {
  cat(sprintf("[ERROR] Rscript Subprocess thất bại: %s\n", e$message))
  quit(status = 1)
})
