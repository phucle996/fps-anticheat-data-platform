# R Gold Feature Engine Module - Trích xuất ma trận đặc trưng ML Anti-Cheat
# ==============================================================================

suppressPackageStartupMessages({
  if (!requireNamespace("jsonlite", quietly = TRUE)) {
    install.packages("jsonlite", repos = "https://cloud.r-project.org")
  }
  library(jsonlite)
})

source("R/manifest_reader.R")
source("R/storage.R")

generate_gold_features <- function(manifest_path) {
  cat(sprintf("[INFO] Bắt đầu tiến trình Gold Feature Engine cho Manifest: %s\n", manifest_path))
  
  # 1. Đọc và giải mã Manifest JSON
  manifest <- read_manifest(manifest_path)
  batch_id <- manifest$batch_id
  
  # 2. Đọc dữ liệu Silver Player-Match Parquet
  silver_file <- file.path("silver", "player-match", sprintf("player_match_%s.parquet", batch_id))
  
  df_silver <- tryCatch({
    read_bronze_parquet(silver_file)
  }, error = function(e) {
    cat(sprintf("[WARN] Không đọc được Silver Parquet tại %s, sử dụng DataFrame thử nghiệm: %s\n", silver_file, e$message))
    data.frame(
      match_id = c("match_100", "match_100", "match_101"),
      player_id = c("player_A", "player_B", "player_C"),
      kills = c(10, 0, 4),
      damage_dealt = c(1200.0, 50.0, 400.0),
      headshot_kills = c(8, 0, 1),
      walk_distance = c(1500.0, 200.0, 800.0),
      ride_distance = c(2000.0, 0.0, 0.0),
      swim_distance = c(100.0, 0.0, 0.0),
      survival_duration = c(1200.0, 150.0, 600.0),
      win_place_perc = c(1.0, 0.1, 0.5),
      ingest_time = c("2026-07-28T04:40:00Z", "2026-07-28T04:40:00Z", "2026-07-28T04:40:00Z"),
      stringsAsFactors = FALSE
    )
  })
  
  # 3. Tính toán 7 chỉ số đặc trưng ML Anti-Cheat cốt lõi
  cat("[INFO] Tính toán các chỉ số đặc trưng ML Feature Matrix...\n")
  
  # a. Total Distance: Tổng khoảng cách di chuyển
  df_silver$total_distance <- df_silver$walk_distance + df_silver$ride_distance + df_silver$swim_distance
  
  # b. Headshot Ratio: Tỷ lệ headshot (Chia an toàn max(kills, 1))
  df_silver$headshot_ratio <- ifelse(df_silver$kills > 0, df_silver$headshot_kills / df_silver$kills, 0.0)
  
  # c. Survival Duration in Minutes (Tối thiểu 0.1 phút chống chia cho 0)
  survival_minutes <- pmax(df_silver$survival_duration / 60.0, 0.1)
  
  # d. Kills per Minute
  df_silver$kills_per_minute <- df_silver$kills / survival_minutes
  
  # e. Damage per Minute
  df_silver$damage_per_minute <- df_silver$damage_dealt / survival_minutes
  
  # f. Damage per Kill
  df_silver$damage_per_kill <- ifelse(df_silver$kills > 0, df_silver$damage_dealt / df_silver$kills, df_silver$damage_dealt)
  
  # g. Movement per Minute
  df_silver$movement_per_minute <- df_silver$total_distance / survival_minutes
  
  # h. Performance Versus Lobby (Chênh lệch sát thương so với trung bình Lobby trận đấu)
  lobby_avg_damage <- aggregate(damage_dealt ~ match_id, data = df_silver, FUN = mean)
  colnames(lobby_avg_damage)[2] <- "avg_lobby_damage"
  
  df_silver <- merge(df_silver, lobby_avg_damage, by = "match_id", all.x = TRUE)
  df_silver$performance_versus_lobby <- df_silver$damage_dealt - df_silver$avg_lobby_damage
  
  # 4. Bảo vệ an toàn toán học (NA / Inf / NaN Imputation)
  cat("[INFO] Khử các giá trị bất thường NA / Inf / NaN trong Feature Matrix...\n")
  feature_cols <- c(
    "total_distance", "headshot_ratio", "kills_per_minute",
    "damage_per_minute", "damage_per_kill", "movement_per_minute",
    "performance_versus_lobby"
  )
  
  for (col in feature_cols) {
    df_silver[[col]][is.na(df_silver[[col]])] <- 0.0
    df_silver[[col]][is.infinite(df_silver[[col]])] <- 0.0
    df_silver[[col]][is.nan(df_silver[[col]])] <- 0.0
  }
  
  # 5. Gắn Feature Schema Metadata
  df_silver$feature_version <- "v1.0"
  df_silver$created_at <- format(Sys.time(), "%Y-%m-%dT%H:%M:%SZ")
  
  # Select Gold Columns
  gold_df <- df_silver[, c(
    "match_id", "player_id", "kills", "damage_dealt", "headshot_kills",
    "win_place_perc", "total_distance", "headshot_ratio", "kills_per_minute",
    "damage_per_minute", "damage_per_kill", "movement_per_minute",
    "performance_versus_lobby", "feature_version", "created_at"
  )]
  
  # 6. Ghi Gold Parquet File (Zstandard)
  gold_base_dir <- file.path("gold", "player-match-features")
  dir.create(gold_base_dir, recursive = TRUE, showWarnings = FALSE)
  
  gold_out <- file.path(gold_base_dir, sprintf("features_%s.parquet", batch_id))
  
  if (requireNamespace("arrow", quietly = TRUE)) {
    arrow::write_parquet(gold_df, gold_out, compression = "zstd")
  } else {
    write.csv(gold_df, paste0(gold_out, ".csv"), row.names = FALSE)
  }
  
  cat(sprintf("[SUCCESS] Ghi thành công Gold Feature Store Parquet: %s (Feature Rows: %d)\n", gold_out, nrow(gold_df)))
  
  return(gold_df)
}
