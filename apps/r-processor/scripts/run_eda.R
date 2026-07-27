#!/usr/bin/env Rscript

# ==============================================================================
# Rscript CLI Entrypoint - Khởi tạo báo cáo phân tích khám phá dữ liệu EDA Report
# ==============================================================================

cat("[INFO] Khởi chạy tiến trình phân tích EDA (Exploratory Data Analysis)...\n")

source("R/storage.R")
source("R/eda_analyzer.R")

# Đọc tham số tùy chọn file Gold Parquet
args <- commandArgs(trailingOnly = TRUE)
gold_path <- if (length(args) > 0) args[1] else NULL

tryCatch({
  report <- generate_eda_report(gold_path)
  cat(sprintf("[SUCCESS] Hoàn tất phân tích EDA! Tổng số bản ghi: %d, Players: %d, Matches: %d\n",
              report$total_records, report$unique_players, report$unique_matches))
  quit(status = 0)
}, error = function(e) {
  cat(sprintf("[ERROR] Tiến trình phân tích EDA thất bại: %s\n", e$message))
  quit(status = 1)
})
