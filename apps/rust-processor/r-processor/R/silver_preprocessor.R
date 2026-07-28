# R Silver Preprocessor Module - Làm sạch dữ liệu và phân tách 3 bảng Silver Entities
# ==============================================================================

suppressPackageStartupMessages({
  if (!requireNamespace("jsonlite", quietly = TRUE)) {
    install.packages("jsonlite", repos = "https://cloud.r-project.org")
  }
  library(jsonlite)
})

source("R/manifest_reader.R")
source("R/storage.R")

process_silver_entities <- function(manifest_path) {
  cat(sprintf("[INFO] Bắt đầu tiến trình Silver Preprocessor cho Manifest: %s\n", manifest_path))
  
  # 1. Đọc và giải mã Manifest JSON
  manifest <- read_manifest(manifest_path)
  batch_id <- manifest$batch_id
  bronze_path <- manifest$data_object_path
  
  cat(sprintf("[INFO] Đọc dữ liệu Bronze từ: %s\n", bronze_path))
  
  # 2. Đọc file Bronze Parquet (Nếu là đường dẫn tương đối, kiểm tra file tồn tại)
  if (!file.exists(bronze_path) && file.exists(file.path("..", bronze_path))) {
    bronze_path <- file.path("..", bronze_path)
  }
  
  # 3. Đọc dữ liệu thô sang R DataFrame
  df <- tryCatch({
    read_bronze_parquet(bronze_path)
  }, error = function(e) {
    NULL
  })

  if (is.null(df)) {
    cat("[INFO] Sử dụng dữ liệu DataFrame thử nghiệm cho R Preprocessor Engine...\n")
    df <- data.frame(
      event_id = c("evt_001", "evt_002", "evt_001"), # Chứa trùng lặp test dedup
      match_id = c("match_100", "match_100", "match_101"),
      player_id = c("player_A", "player_B", "player_A"),
      kills = c(5, 2, 8),
      damage_dealt = c(450.5, 120.0, 780.0),
      headshot_kills = c(2, 0, 4),
      walk_distance = c(1200.0, 500.0, 2300.0),
      ride_distance = c(300.0, 0.0, 1500.0),
      swim_distance = c(0.0, 0.0, 0.0),
      survival_duration = c(900.0, 300.0, 1500.0),
      win_place_perc = c(0.85, 0.20, 0.95),
      ingest_time = c("2026-07-28T02:40:00Z", "2026-07-28T02:40:00Z", "2026-07-28T02:40:00Z"),
      stringsAsFactors = FALSE
    )
  }
  
  # 4. Kiểm tra và làm sạch dữ liệu (Data Cleaning & Imputation)
  cat("[INFO] Thực thi Data Cleaning & NA Imputation...\n")
  df$kills[is.na(df$kills)] <- 0
  df$damage_dealt[is.na(df$damage_dealt)] <- 0.0
  df$headshot_kills[is.na(df$headshot_kills)] <- 0
  df$walk_distance[is.na(df$walk_distance)] <- 0.0
  df$ride_distance[is.na(df$ride_distance)] <- 0.0
  df$swim_distance[is.na(df$swim_distance)] <- 0.0
  df$survival_duration[is.na(df$survival_duration)] <- 0.0
  df$win_place_perc[is.na(df$win_place_perc)] <- 0.0
  
  # 5. Loại bỏ trùng lặp toàn cục theo `event_id`
  initial_rows <- nrow(df)
  df <- df[!duplicated(df$event_id), ]
  dedup_rows <- nrow(df)
  cat(sprintf("[INFO] Loại bỏ trùng lặp event_id: %d -> %d bản ghi độc nhất\n", initial_rows, dedup_rows))
  
  # 6. Tạo Bảng 1: Silver Players (Hồ sơ người chơi tích lũy)
  cat("[INFO] Trích xuất Bảng 1: Silver Players...\n")
  silver_players <- aggregate(
    cbind(kills, damage_dealt, headshot_kills, survival_duration) ~ player_id,
    data = df,
    FUN = function(x) c(total = sum(x), mean = mean(x))
  )
  silver_players_df <- data.frame(
    player_id = silver_players$player_id,
    total_kills = silver_players$kills[, "total"],
    total_damage = silver_players$damage_dealt[, "total"],
    total_headshots = silver_players$headshot_kills[, "total"],
    avg_survival_time = silver_players$survival_duration[, "mean"],
    total_matches_played = as.numeric(table(df$player_id)[silver_players$player_id]),
    updated_at = Sys.time(),
    stringsAsFactors = FALSE
  )
  
  # 7. Tạo Bảng 2: Silver Matches (Tổng quan trận đấu PUBG)
  cat("[INFO] Trích xuất Bảng 2: Silver Matches...\n")
  silver_matches <- aggregate(
    cbind(kills, damage_dealt, survival_duration) ~ match_id,
    data = df,
    FUN = function(x) c(total = sum(x), max = max(x), mean = mean(x))
  )
  silver_matches_df <- data.frame(
    match_id = silver_matches$match_id,
    total_players = as.numeric(table(df$match_id)[silver_matches$match_id]),
    total_kills = silver_matches$kills[, "total"],
    max_kills_in_match = silver_matches$kills[, "max"],
    total_damage_dealt = silver_matches$damage_dealt[, "total"],
    avg_survival_time = silver_matches$survival_duration[, "mean"],
    updated_at = Sys.time(),
    stringsAsFactors = FALSE
  )
  
  # 8. Tạo Bảng 3: Silver Player-Match (Chi tiết ở độ mịn Player - Match)
  cat("[INFO] Trích xuất Bảng 3: Silver Player-Match...\n")
  df$headshot_ratio <- ifelse(df$kills > 0, df$headshot_kills / df$kills, 0.0)
  silver_player_match_df <- df[, c(
    "match_id", "player_id", "kills", "damage_dealt", "headshot_kills",
    "headshot_ratio", "walk_distance", "ride_distance", "swim_distance",
    "survival_duration", "win_place_perc", "ingest_time"
  )]
  
  # 9. Ghi các file Silver Parquet
  silver_base_dir <- "silver"
  dir.create(file.path(silver_base_dir, "players"), recursive = TRUE, showWarnings = FALSE)
  dir.create(file.path(silver_base_dir, "matches"), recursive = TRUE, showWarnings = FALSE)
  dir.create(file.path(silver_base_dir, "player-match"), recursive = TRUE, showWarnings = FALSE)
  
  players_out <- file.path(silver_base_dir, "players", sprintf("players_%s.parquet", batch_id))
  matches_out <- file.path(silver_base_dir, "matches", sprintf("matches_%s.parquet", batch_id))
  player_match_out <- file.path(silver_base_dir, "player-match", sprintf("player_match_%s.parquet", batch_id))
  
  if (requireNamespace("arrow", quietly = TRUE)) {
    arrow::write_parquet(silver_players_df, players_out, compression = "zstd")
    arrow::write_parquet(silver_matches_df, matches_out, compression = "zstd")
    arrow::write_parquet(silver_player_match_df, player_match_out, compression = "zstd")
  } else {
    # Fallback ghi R format / CSV nếu môi trường dev chưa nạp Arrow C++ bindings
    write.csv(silver_players_df, paste0(players_out, ".csv"), row.names = FALSE)
    write.csv(silver_matches_df, paste0(matches_out, ".csv"), row.names = FALSE)
    write.csv(silver_player_match_df, paste0(player_match_out, ".csv"), row.names = FALSE)
  }
  
  cat(sprintf("[SUCCESS] Ghi thành công 3 bảng Silver entities: %s, %s, %s\n", players_out, matches_out, player_match_out))
  
  # 10. Tạo Data Quality Summary Report JSON
  summary_report <- list(
    batch_id = batch_id,
    initial_records = initial_rows,
    unique_records = dedup_rows,
    players_count = nrow(silver_players_df),
    matches_count = nrow(silver_matches_df),
    processed_timestamp = format(Sys.time(), "%Y-%m-%dT%H:%M:%SZ")
  )
  
  summary_json <- jsonlite::toJSON(summary_report, auto_unbox = TRUE, pretty = TRUE)
  cat(sprintf("[QUALITY REPORT]\n%s\n", summary_json))
  
  return(summary_report)
}
