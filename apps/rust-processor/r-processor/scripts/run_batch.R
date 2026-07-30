#!/usr/bin/env Rscript

# ==============================================================================
# Rscript CLI Entrypoint - Nhận local paths từ Rust Worker
# Args:
#   --manifest <path>    : Local path tới file manifest JSON (đã download từ MinIO bởi Rust)
#   --bronze <path>      : Local path tới file Bronze Parquet (đã download từ MinIO bởi Rust)
#   --output-dir <path>  : Local temp directory để ghi Silver/Gold output (Rust sẽ upload)
#
# Lưu ý: R KHÔNG đọc/ghi MinIO trực tiếp — Rust chịu trách nhiệm download/upload
# ==============================================================================

# Parse tham số CLI
args <- commandArgs(trailingOnly = TRUE)

manifest_path <- NULL
bronze_path   <- NULL
output_dir    <- NULL

i <- 1
while (i <= length(args)) {
  if (args[i] == "--manifest" && i < length(args)) {
    manifest_path <- args[i + 1]
    i <- i + 2
  } else if (args[i] == "--bronze" && i < length(args)) {
    bronze_path <- args[i + 1]
    i <- i + 2
  } else if (args[i] == "--output-dir" && i < length(args)) {
    output_dir <- args[i + 1]
    i <- i + 2
  } else {
    i <- i + 1
  }
}

# Validate tham số bắt buộc
if (is.null(manifest_path)) {
  cat("[ERROR] Bắt buộc truyền tham số `--manifest <path>`!\n")
  quit(status = 1)
}
if (is.null(bronze_path)) {
  cat("[ERROR] Bắt buộc truyền tham số `--bronze <path>`!\n")
  quit(status = 1)
}
if (is.null(output_dir)) {
  cat("[ERROR] Bắt buộc truyền tham số `--output-dir <path>`!\n")
  quit(status = 1)
}

# Kiểm tra file tồn tại thực sự (Fail-Close: không fallback dữ liệu mẫu)
if (!file.exists(manifest_path)) {
  cat(sprintf("[ERROR] File manifest không tồn tại tại đường dẫn local: %s\n", manifest_path))
  quit(status = 1)
}
if (!file.exists(bronze_path)) {
  cat(sprintf("[ERROR] File Bronze Parquet không tồn tại tại đường dẫn local: %s\n", bronze_path))
  quit(status = 1)
}

cat(sprintf("[INFO] Rscript CLI nhận yêu cầu xử lý:\n"))
cat(sprintf("  manifest   : %s\n", manifest_path))
cat(sprintf("  bronze     : %s\n", bronze_path))
cat(sprintf("  output_dir : %s\n", output_dir))

# Nạp các module R cốt lõi (đường dẫn tương đối từ r-processor/)
source("R/config.R")
source("R/manifest_reader.R")
source("R/storage.R")
source("R/silver_preprocessor.R")
source("R/gold_feature_engine.R")

tryCatch({
  # 1. Đọc và kiểm tra Manifest JSON local (đã download bởi Rust)
  manifest <- read_manifest(manifest_path)
  cat(sprintf("[SUCCESS] Đã đọc manifest Batch ID '%s' (Total Records: %d)\n",
              manifest$batch_id, manifest$total_records_read))

  # 2. Xử lý tiền xử lý Silver Layer Entities từ Bronze Parquet local
  silver_report <- process_silver_entities(manifest_path, bronze_path, output_dir)

  # 3. Trích xuất Gold Layer Feature Matrix từ Silver output
  gold_df <- generate_gold_features(manifest_path, output_dir)

  cat(sprintf("[SUCCESS] Hoàn tất R Pipeline Batch '%s' (Silver Players: %d, Gold Features: %d)\n",
              silver_report$batch_id, silver_report$players_count, nrow(gold_df)))
  quit(status = 0)
}, error = function(e) {
  message <- sprintf("[ERROR] Rscript Subprocess thất bại: %s\n", e$message)
  cat(message, file = stderr())
  cat(message)
  quit(status = 1)
})
