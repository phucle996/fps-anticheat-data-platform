#!/usr/bin/env Rscript

# ==============================================================================
# Spark-Style Persistent Dynamic R Worker Daemon
# Nạp sẵn thư viện 1 lần duy nhất, đọc task từ stdin, 5s Idle Timeout auto-shutdown
# ==============================================================================
# Format stdin message (mỗi dòng = 1 task):
#   <manifest_local_path>|<bronze_local_path>|<output_dir>
# Rust spawner đã download manifest và bronze từ MinIO trước khi gửi vào stdin
# R chỉ đọc local files — không kết nối MinIO trực tiếp

cat("[INFO] Khởi động R Worker Daemon Process (Pre-loading libraries into RAM)...\n")

# Nạp sẵn các thư viện R cốt lõi vào bộ nhớ RAM (warm up)
source("R/config.R")
source("R/manifest_reader.R")
source("R/storage.R")
source("R/silver_preprocessor.R")
source("R/gold_feature_engine.R")

cat("[SUCCESS] Đã nạp thành công 100% R libraries vào RAM. Sẵn sàng nhận batch task!\n")

stdin_con      <- file("stdin", "r")
idle_seconds   <- 0
IDLE_TIMEOUT_SEC <- 5 # 5s Idle Timeout (1-2 chu kỳ batch)

while (TRUE) {
  # Đọc 1 dòng từ stdin
  line <- readLines(stdin_con, n = 1, warn = FALSE)

  if (length(line) > 0 && nchar(trimws(line)) > 0) {
    # Parse format: "<manifest_path>|<bronze_path>|<output_dir>"
    parts <- strsplit(trimws(line), "\\|")[[1]]

    if (length(parts) < 3) {
      cat(sprintf("[ERROR] Format stdin không hợp lệ. Cần: manifest|bronze|output_dir. Nhận: %s\n", line))
      idle_seconds <- 0
      next
    }

    manifest_path <- parts[1]
    bronze_path   <- parts[2]
    output_dir    <- parts[3]

    cat(sprintf("[WARM WORKER] Nhận task mới (0ms Startup Delay):\n"))
    cat(sprintf("  manifest: %s\n", manifest_path))
    cat(sprintf("  bronze  : %s\n", bronze_path))
    cat(sprintf("  output  : %s\n", output_dir))

    # Reset idle timer
    idle_seconds <- 0

    tryCatch({
      # 1. Silver preprocessing (aggregate + clean)
      report <- process_silver_entities(manifest_path, bronze_path, output_dir)
      # 2. Gold feature extraction (trên Silver đã aggregate)
      gold_df <- generate_gold_features(manifest_path, output_dir)

      cat(sprintf("[TASK SUCCESS] Batch '%s': Silver(%d rows) & Gold(%d features) hoàn tất!\n",
                  report$batch_id, report$player_match_rows, nrow(gold_df)))
    }, error = function(e) {
      cat(sprintf("[TASK ERROR] Lỗi xử lý batch manifest '%s': %s\n", manifest_path, e$message))
    })

  } else {
    # Không có task mới — đếm idle
    Sys.sleep(1)
    idle_seconds <- idle_seconds + 1

    if (idle_seconds >= IDLE_TIMEOUT_SEC) {
      cat(sprintf("[IDLE TIMEOUT %ds] Tự động đóng R Worker Daemon giải phóng RAM...\n",
                  IDLE_TIMEOUT_SEC))
      break
    }
  }
}

close(stdin_con)
cat("[INFO] R Worker Daemon kết thúc an toàn.\n")
quit(status = 0)
