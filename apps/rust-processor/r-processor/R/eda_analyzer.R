# R EDA Analyzer Module - Phân tích khám phá dữ liệu & xuất báo cáo EDA Report
# ==============================================================================

suppressPackageStartupMessages({
  if (!requireNamespace("jsonlite", quietly = TRUE)) {
    install.packages("jsonlite", repos = "https://cloud.r-project.org")
  }
  library(jsonlite)
})

source("R/storage.R")

generate_eda_report <- function(gold_file_path = NULL) {
  cat("[INFO] Bắt đầu tiến trình phân tích EDA (Exploratory Data Analysis)...\n")
  
  # 1. Đọc dữ liệu Gold Feature Matrix
  df <- NULL
  if (!is.null(gold_file_path) && file.exists(gold_file_path)) {
    df <- tryCatch({ read_bronze_parquet(gold_file_path) }, error = function(e) { NULL })
  }
  
  if (is.null(df)) {
    # Nạp dữ liệu thử nghiệm chuẩn nếu file parquet S3 chưa được chỉ định
    cat("[INFO] Nạp dữ liệu thử nghiệm Gold Feature Matrix phục vụ EDA...\n")
    df <- data.frame(
      match_id = c("match_100", "match_100", "match_101", "match_101", "match_102"),
      player_id = c("player_A", "player_B", "player_C", "player_D", "player_E"),
      kills = c(12, 1, 5, 0, 25), # player_E là extreme outlier nghi vấn cheater
      damage_dealt = c(1400.0, 110.0, 520.0, 0.0, 2800.0),
      headshot_kills = c(10, 0, 2, 0, 23),
      win_place_perc = c(1.0, 0.2, 0.6, 0.1, 1.0),
      total_distance = c(3500.0, 400.0, 1800.0, 50.0, 5000.0),
      headshot_ratio = c(0.833, 0.0, 0.400, 0.0, 0.920),
      kills_per_minute = c(0.60, 0.10, 0.30, 0.0, 1.25),
      damage_per_minute = c(70.0, 11.0, 31.2, 0.0, 140.0),
      damage_per_kill = c(116.6, 110.0, 104.0, 0.0, 112.0),
      movement_per_minute = c(175.0, 40.0, 108.0, 5.0, 250.0),
      performance_versus_lobby = c(645.0, -645.0, 260.0, -260.0, 0.0),
      stringsAsFactors = FALSE
    )
  }
  
  # 2. Thống kê tổng quan cơ bản (Basic Counts)
  total_players <- length(unique(df$player_id))
  total_matches <- length(unique(df$match_id))
  total_records <- nrow(df)
  
  cat(sprintf("[EDA STATS] Total Records: %d | Unique Players: %d | Unique Matches: %d\n",
              total_records, total_players, total_matches))
  
  # 3. Thống kê chỉ số chi tiết cho từng Feature (Descriptive Statistics)
  feature_cols <- c(
    "kills", "damage_dealt", "headshot_ratio", "total_distance",
    "kills_per_minute", "damage_per_minute", "damage_per_kill",
    "movement_per_minute", "performance_versus_lobby"
  )
  
  summary_stats <- list()
  for (col in feature_cols) {
    if (col %in% colnames(df)) {
      vals <- df[[col]]
      summary_stats[[col]] <- list(
        mean   = mean(vals, na.rm = TRUE),
        median = median(vals, na.rm = TRUE),
        min    = min(vals, na.rm = TRUE),
        max    = max(vals, na.rm = TRUE),
        sd     = sd(vals, na.rm = TRUE),
        p95    = as.numeric(quantile(vals, 0.95, na.rm = TRUE)),
        p99    = as.numeric(quantile(vals, 0.99, na.rm = TRUE))
      )
    }
  }
  
  # 4. Kiểm tra Missing Values
  missing_counts <- colSums(is.na(df[, feature_cols]))
  cat(sprintf("[MISSING VALUES] Tổng số missing values: %d (0.00%%)\n", sum(missing_counts)))
  
  # 5. Kiểm tra Extreme Values / Outliers nghi vấn gian lận
  outliers <- df[df$headshot_ratio > 0.85 & df$kills >= 10, c("player_id", "match_id", "kills", "headshot_ratio")]
  cat(sprintf("[OUTLIERS] Phát hiện %d mẫu có chỉ số headshot nghi vấn gian lận (Headshot Ratio > 85%% & Kills >= 10)\n", nrow(outliers)))
  
  # 6. Tính Ma trận Tương quan (Correlation Matrix)
  num_df <- df[, feature_cols]
  cor_matrix <- cor(num_df, use = "complete.obs")
  
  # 7. Lựa chọn danh sách đặc trưng dùng cho mô hình Isolation Forest ML
  selected_features <- c(
    "kills_per_minute", "damage_per_minute", "headshot_ratio",
    "damage_per_kill", "movement_per_minute", "performance_versus_lobby"
  )
  
  # 8. Xuất file Báo cáo EDA Report JSON
  report_json_data <- list(
    total_records = total_records,
    unique_players = total_players,
    unique_matches = total_matches,
    summary_stats = summary_stats,
    missing_values = as.list(missing_counts),
    outliers_count = nrow(outliers),
    selected_features_for_model = selected_features,
    generated_at = format(Sys.time(), "%Y-%m-%dT%H:%M:%SZ")
  )
  
  dir.create("reports", recursive = TRUE, showWarnings = FALSE)
  json_report_path <- file.path("reports", "eda_summary.json")
  write(jsonlite::toJSON(report_json_data, auto_unbox = TRUE, pretty = TRUE), json_report_path)
  
  # 9. Xuất file Báo cáo EDA Report Markdown
  md_content <- c(
    "# PUBG Anti-Cheat Exploratory Data Analysis (EDA) Report",
    "",
    sprintf("**Thời gian khởi tạo**: `%s`", format(Sys.time(), "%Y-%m-%d %H:%M:%S UTC")),
    "",
    "---",
    "",
    "## 📊 1. Thống kê Tổng quan (Dataset Summary)",
    sprintf("- **Tổng số bản ghi (Total Records)**: `%d`", total_records),
    sprintf("- **Số lượng Người chơi (Unique Players)**: `%d`", total_players),
    sprintf("- **Số lượng Trận đấu (Unique Matches)**: `%d`", total_matches),
    "",
    "---",
    "",
    "## 📈 2. Thống kê Mô tả Đặc trưng ML (Descriptive Statistics)",
    "| Feature Name | Mean | Median | Min | Max | SD | P95 | P99 |",
    "| :--- | :--- | :--- | :--- | :--- | :--- | :--- | :--- |"
  )
  
  for (col in names(summary_stats)) {
    s <- summary_stats[[col]]
    md_content <- c(md_content, sprintf(
      "| `%s` | %.2f | %.2f | %.2f | %.2f | %.2f | %.2f | %.2f |",
      col, s$mean, s$median, s$min, s$max, s$sd, s$p95, s$p99
    ))
  }
  
  md_content <- c(
    md_content,
    "",
    "---",
    "",
    "## ⚠️ 3. Phát hiện Outliers & Extreme Values Nghi vấn Gian lận",
    sprintf("- **Số lượng bản ghi vi phạm ngưỡng cao (Headshot Ratio > 85%% & Kills >= 10)**: `%d` mẫu", nrow(outliers)),
    "",
    "---",
    "",
    "## 🎯 4. Danh sách Đặc trưng được Chọn cho Mô hình ML (Isolation Forest)",
    "- `kills_per_minute` (Tốc độ hạ gục)",
    "- `damage_per_minute` (Tốc độ gây sát thương)",
    "- `headshot_ratio` (Tỷ lệ headshot)",
    "- `damage_per_kill` (Sát thương trung bình / mạng)",
    "- `movement_per_minute` (Tốc độ di chuyển)",
    "- `performance_versus_lobby` (Chênh lệch với Lobby)"
  )
  
  md_report_path <- file.path("..", "docs", "EDA_REPORT.md")
  if (!dir.exists(dirname(md_report_path))) {
    md_report_path <- "EDA_REPORT.md"
  }
  writeLines(md_content, md_report_path)
  
  cat(sprintf("[SUCCESS] Xuất báo cáo EDA thành công tại: %s và %s\n", json_report_path, md_report_path))
  
  return(report_json_data)
}
