#!/usr/bin/env Rscript

# ==============================================================================
# Spark-Style Persistent Dynamic R Worker Daemon
# Nạp sẵn thư viện 1 lần duy nhất, đọc task từ stdin, 5s Idle Timeout auto-shutdown
# ==============================================================================

cat("[INFO] Khởi động R Worker Daemon Process (Pre-loading libraries into RAM)...\n")

# Nạp sẵn các thư viện R cốt lõi vào bộ nhớ RAM
source("R/config.R")
source("R/manifest_reader.R")
source("R/storage.R")
source("R/silver_preprocessor.R")

cat("[SUCCESS] Đã nạp thành công 100% R libraries vào RAM. Sẵn sàng nhận batch task!\n")

stdin_con <- file("stdin", "r")
idle_seconds <- 0
IDLE_TIMEOUT_SEC <- 5 # Cấu hình 5s Idle Timeout (1-2 chu kỳ batch)

while (TRUE) {
  # Đọc 1 dòng từ stdin không chặn
  line <- readLines(stdin_con, n = 1, warn = FALSE)
  
  if (length(line) > 0 && nchar(trimws(line)) > 0) {
    manifest_path <- trimws(line)
    cat(sprintf("[WARM WORKER] Đã nhận task mới: %s (0ms Startup Delay)\n", manifest_path))
    
    # Reset thời gian idle
    idle_seconds <- 0
    
    tryCatch({
      report <- process_silver_entities(manifest_path)
      cat(sprintf("[TASK SUCCESS] Bảng Silver Entities cho batch '%s' xử lý xong!\n", report$batch_id))
    }, error = function(e) {
      cat(sprintf("[TASK ERROR] Lỗi xử lý batch manifest '%s': %s\n", manifest_path, e$message))
    })
  } else {
    # Nếu không có dòng mới, nghỉ 1 giây và đếm thời gian idle
    Sys.sleep(1)
    idle_seconds <- idle_seconds + 1
    
    if (idle_seconds >= IDLE_TIMEOUT_SEC) {
      cat(sprintf("[IDLE TIMEOUT %ds] Ngưng có dữ liệu thô trong %d giây, tự động đóng R Worker Daemon giải phóng RAM...\n", 
                  IDLE_TIMEOUT_SEC, IDLE_TIMEOUT_SEC))
      break
    }
  }
}

close(stdin_con)
cat("[INFO] R Worker Daemon kết thúc an toàn.\n")
quit(status = 0)
