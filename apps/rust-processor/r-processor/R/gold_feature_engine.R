# R Gold Feature Engine Module - Trích xuất ma trận đặc trưng ML Anti-Cheat
# ==============================================================================
# Nhận local paths từ run_batch.R:
#   manifest_path : local path tới manifest JSON
#   output_dir    : local temp dir chứa Silver output (từ silver_preprocessor)
#                   và nơi ghi Gold output (Rust upload sau)
# KHÔNG đọc/ghi MinIO trực tiếp
# KHÔNG fallback mock data — Fail-Close nếu Silver Player-Match không tồn tại

suppressPackageStartupMessages({
  if (!requireNamespace("jsonlite", quietly = TRUE)) {
    install.packages("jsonlite", repos = "https://cloud.r-project.org")
  }
  library(jsonlite)
})

source("R/manifest_reader.R")
source("R/storage.R")

generate_gold_features <- function(manifest_path, output_dir) {
  cat(sprintf("[INFO] Gold Feature Engine bắt đầu. Output dir: %s\n", output_dir))

  # 1. Đọc Manifest JSON để lấy batch_id
  manifest <- read_manifest(manifest_path)
  batch_id <- manifest$batch_id

  # 2. Đọc Silver Player-Match Parquet từ output_dir (đã được silver_preprocessor tạo ra)
  # Đây là dữ liệu ĐÃ AGGREGATE: mỗi dòng = 1 player trong 1 trận, kills = tổng kills
  silver_pm_path <- file.path(output_dir, "silver", "player-match",
                               sprintf("player_match_%s.parquet", batch_id))

  cat(sprintf("[INFO] Đọc Silver Player-Match từ: %s\n", silver_pm_path))

  # Fail-Close: nếu Silver không tồn tại → Gold không thể tạo được → dừng với lỗi rõ ràng
  if (!file.exists(silver_pm_path)) {
    # Fallback thử CSV (nếu arrow không có sẵn khi chạy silver_preprocessor)
    silver_pm_csv <- paste0(silver_pm_path, ".csv")
    if (file.exists(silver_pm_csv)) {
      df_silver <- read.csv(silver_pm_csv, stringsAsFactors = FALSE)
      cat(sprintf("[INFO] Đọc Silver từ CSV fallback: %s (%d rows)\n",
                  silver_pm_csv, nrow(df_silver)))
    } else {
      stop(sprintf(
        "[ERROR] Silver Player-Match không tồn tại: %s\n" \
        "Kiểm tra silver_preprocessor đã chạy thành công và có dữ liệu thật.",
        silver_pm_path
      ))
    }
  } else {
    df_silver <- read_bronze_parquet(silver_pm_path)
    cat(sprintf("[INFO] Đọc Silver Player-Match thành công: %d player-match records\n",
                nrow(df_silver)))
  }

  # 3. Validate grain: mỗi dòng phải là unique (match_id, player_id)
  n_unique <- nrow(unique(df_silver[, c("match_id", "player_id")]))
  if (n_unique != nrow(df_silver)) {
    stop(sprintf(
      "[ERROR] Silver Player-Match chưa được aggregate đúng: %d rows nhưng chỉ %d unique (match_id, player_id). " \
      "Kiểm tra aggregate_player_match() trong silver_preprocessor.",
      nrow(df_silver), n_unique
    ))
  }
  cat(sprintf("[INFO] Grain validation OK: %d unique player-match records\n", n_unique))

  # 4. Tính toán 6 chỉ số đặc trưng ML Feature Contract (đúng ngữ nghĩa)
  cat("[INFO] Tính toán Gold Feature Matrix...\n")

  # Survival time tính bằng phút (tối thiểu 0.1 phút chống chia 0)
  survival_minutes <- pmax(df_silver$survival_duration / 60.0, 0.1)

  # a. kills_per_minute: tốc độ giết (kills = đã aggregate, tổng kills trong trận)
  df_silver$kills_per_minute <- df_silver$kills / survival_minutes

  # b. damage_per_minute: tốc độ gây sát thương
  # Lưu ý: damage_dealt = 0 trong kill-event schema → feature này ít có ý nghĩa cho match_deaths data
  df_silver$damage_per_minute <- df_silver$damage_dealt / survival_minutes

  # c. headshot_ratio: tỷ lệ headshot trên tổng kills (đã aggregate)
  df_silver$headshot_ratio <- ifelse(
    df_silver$kills > 0,
    df_silver$headshot_kills / df_silver$kills,
    0.0
  )

  # d. damage_per_kill: sát thương trung bình mỗi kill
  df_silver$damage_per_kill <- ifelse(
    df_silver$kills > 0,
    df_silver$damage_dealt / df_silver$kills,
    df_silver$damage_dealt  # 0 nếu kills=0
  )

  # e. movement_per_minute: di chuyển tổng thể / thời gian sống
  df_silver$total_distance    <- df_silver$walk_distance + df_silver$ride_distance + df_silver$swim_distance
  df_silver$movement_per_minute <- df_silver$total_distance / survival_minutes

  # f. performance_versus_lobby: chênh lệch kills so với trung bình trận (lobby-relative anomaly)
  # Đây là feature quan trọng nhất cho anti-cheat: outlier kills trong lobby
  lobby_avg_kills <- aggregate(kills ~ match_id, data = df_silver, FUN = mean)
  colnames(lobby_avg_kills)[2] <- "avg_lobby_kills"
  df_silver <- merge(df_silver, lobby_avg_kills, by = "match_id", all.x = TRUE)
  df_silver$performance_versus_lobby <- df_silver$kills - df_silver$avg_lobby_kills

  # 5. Bảo vệ toán học (NA / Inf / NaN Imputation)
  cat("[INFO] Khử NA / Inf / NaN trong Feature Matrix...\n")
  feature_cols <- c(
    "kills_per_minute", "damage_per_minute", "headshot_ratio",
    "damage_per_kill", "movement_per_minute", "performance_versus_lobby"
  )
  for (col in feature_cols) {
    df_silver[[col]][is.na(df_silver[[col]])]       <- 0.0
    df_silver[[col]][is.infinite(df_silver[[col]])] <- 0.0
    df_silver[[col]][is.nan(df_silver[[col]])]      <- 0.0
  }

  # 6. Gắn Feature Schema Metadata
  df_silver$feature_version <- "v1.0"
  df_silver$created_at      <- format(Sys.time(), "%Y-%m-%dT%H:%M:%SZ")

  # 7. Select Gold Columns — đúng 6 ML features + identifiers
  gold_df <- df_silver[, c(
    "match_id", "player_id",
    "kills", "damage_dealt", "headshot_kills",  # raw fields để debugging
    "win_place_perc",
    # 6 ML Feature Contract:
    "kills_per_minute",         # Feature 1
    "damage_per_minute",        # Feature 2
    "headshot_ratio",           # Feature 3
    "damage_per_kill",          # Feature 4
    "movement_per_minute",      # Feature 5
    "performance_versus_lobby", # Feature 6
    "feature_version", "created_at"
  )]

  # 8. Ghi Gold Parquet File ra output_dir (Rust sẽ upload lên MinIO)
  gold_base_dir <- file.path(output_dir, "gold", "player-match-features")
  dir.create(gold_base_dir, recursive = TRUE, showWarnings = FALSE)

  gold_out <- file.path(gold_base_dir, sprintf("features_%s.parquet", batch_id))

  if (requireNamespace("arrow", quietly = TRUE)) {
    arrow::write_parquet(gold_df, gold_out, compression = "zstd")
  } else {
    # Fallback CSV
    write.csv(gold_df, paste0(gold_out, ".csv"), row.names = FALSE)
    gold_out <- paste0(gold_out, ".csv")
  }

  cat(sprintf("[SUCCESS] Ghi Gold Feature Store: %s (%d player-match features)\n",
              gold_out, nrow(gold_df)))

  return(gold_df)
}
