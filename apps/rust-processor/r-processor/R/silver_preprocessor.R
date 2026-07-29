# R Silver Preprocessor Module - Làm sạch dữ liệu và phân tách 3 bảng Silver Entities
# ==============================================================================
# Nhận local paths từ run_batch.R (đã download từ MinIO bởi Rust):
#   manifest_path : local path tới manifest JSON
#   bronze_path   : local path tới Bronze Parquet
#   output_dir    : local temp dir để ghi Silver output (Rust upload sau)
# KHÔNG đọc/ghi MinIO trực tiếp
# KHÔNG fallback mock data — Fail-Close nếu file không tồn tại

suppressPackageStartupMessages({
  if (!requireNamespace("jsonlite", quietly = TRUE)) {
    install.packages("jsonlite", repos = "https://cloud.r-project.org")
  }
  library(jsonlite)
})

source("R/manifest_reader.R")
source("R/storage.R")

process_silver_entities <- function(manifest_path, bronze_path, output_dir) {
  cat(sprintf("[INFO] Silver Preprocessor bắt đầu. Manifest: %s\n", manifest_path))

  # 1. Đọc và giải mã Manifest JSON (local file đã download)
  manifest <- read_manifest(manifest_path)
  batch_id <- manifest$batch_id

  # 2. Đọc Bronze Parquet từ local path đã download (Fail-Close: không fallback mock)
  cat(sprintf("[INFO] Đọc Bronze Parquet từ local path: %s\n", bronze_path))
  df <- read_bronze_parquet(bronze_path)
  # read_bronze_parquet sẽ stop() nếu file không tồn tại — propagate lên caller

  cat(sprintf("[INFO] Đọc thành công %d records từ Bronze Parquet\n", nrow(df)))

  # 3. Kiểm tra và làm sạch dữ liệu (Data Cleaning & NA Imputation)
  cat("[INFO] Thực thi Data Cleaning & NA Imputation...\n")
  df$kills[is.na(df$kills)]                     <- 0L
  df$damage_dealt[is.na(df$damage_dealt)]       <- 0.0
  df$headshot_kills[is.na(df$headshot_kills)]   <- 0L
  df$walk_distance[is.na(df$walk_distance)]     <- 0.0
  df$ride_distance[is.na(df$ride_distance)]     <- 0.0
  df$swim_distance[is.na(df$swim_distance)]     <- 0.0
  df$survival_duration[is.na(df$survival_duration)] <- 0.0
  if ("win_place_perc" %in% colnames(df)) {
    df$win_place_perc[is.na(df$win_place_perc)] <- 0.0
  }

  # 4. Loại bỏ trùng lặp toàn cục theo `event_id` (Rust đã dedup trong batch, R dedup cross-batch)
  initial_rows <- nrow(df)
  if ("event_id" %in% colnames(df)) {
    df <- df[!duplicated(df$event_id), ]
  }
  dedup_rows <- nrow(df)
  cat(sprintf("[INFO] Dedup event_id: %d -> %d bản ghi độc nhất\n", initial_rows, dedup_rows))

  # ===========================================================================
  # BLOCKER FIX: Aggregate Kill Events thành Player-Match grain
  # ===========================================================================
  # Mỗi dòng Bronze có thể là 1 kill event (Op=OpKillEvent, kills=1) hoặc
  # 1 match summary (Op=OpMatchSummary, kills=total). Cả hai cần được aggregate
  # theo (match_id, player_id) trước khi tạo Silver Player-Match để đảm bảo:
  #   - 1 dòng = 1 player trong 1 trận
  #   - kills = tổng kills của player đó trong trận
  #   - survival_duration = thời gian sống sót (max của các kill events, hoặc giá trị match summary)
  #   - damage_dealt = tổng damage (chỉ có trong match_summary schema)
  # ===========================================================================
  cat("[INFO] Aggregate kill events/match summaries thành player-match grain...\n")
  player_match_raw <- aggregate_player_match(df)
  cat(sprintf("[INFO] Kết quả aggregate: %d kill/summary events -> %d player-match records\n",
              nrow(df), nrow(player_match_raw)))

  # 5. Tạo Bảng 1: Silver Players (Hồ sơ người chơi tích lũy cross-match)
  cat("[INFO] Trích xuất Bảng 1: Silver Players...\n")
  silver_players_df <- data.frame(
    player_id           = tapply(player_match_raw$player_id, player_match_raw$player_id, function(x) x[1]),
    total_kills         = tapply(player_match_raw$kills, player_match_raw$player_id, sum),
    total_damage        = tapply(player_match_raw$damage_dealt, player_match_raw$player_id, sum),
    total_headshots     = tapply(player_match_raw$headshot_kills, player_match_raw$player_id, sum),
    avg_survival_time   = tapply(player_match_raw$survival_duration, player_match_raw$player_id, mean),
    total_matches_played = as.integer(table(player_match_raw$player_id)),
    updated_at          = Sys.time(),
    stringsAsFactors    = FALSE,
    row.names           = NULL
  )

  # 6. Tạo Bảng 2: Silver Matches (Tổng quan trận đấu PUBG)
  cat("[INFO] Trích xuất Bảng 2: Silver Matches...\n")
  silver_matches_df <- data.frame(
    match_id              = names(tapply(player_match_raw$kills, player_match_raw$match_id, sum)),
    total_players         = as.integer(table(player_match_raw$match_id)),
    total_kills           = tapply(player_match_raw$kills, player_match_raw$match_id, sum),
    max_kills_in_match    = tapply(player_match_raw$kills, player_match_raw$match_id, max),
    total_damage_dealt    = tapply(player_match_raw$damage_dealt, player_match_raw$match_id, sum),
    avg_survival_time     = tapply(player_match_raw$survival_duration, player_match_raw$match_id, mean),
    updated_at            = Sys.time(),
    stringsAsFactors      = FALSE,
    row.names             = NULL
  )

  # 7. Tạo Bảng 3: Silver Player-Match (Chi tiết ở độ mịn Player-Match đã aggregate)
  # Đây là bảng CORE: mỗi dòng = 1 player trong 1 trận, kills = tổng kills trong trận đó
  cat("[INFO] Trích xuất Bảng 3: Silver Player-Match (đã aggregate)...\n")
  player_match_raw$headshot_ratio <- ifelse(
    player_match_raw$kills > 0,
    player_match_raw$headshot_kills / player_match_raw$kills,
    0.0
  )
  silver_player_match_df <- player_match_raw[, c(
    "match_id", "player_id", "kills", "damage_dealt", "headshot_kills",
    "headshot_ratio", "walk_distance", "ride_distance", "swim_distance",
    "survival_duration", "win_place_perc"
  )]

  # 8. Tạo Bảng 4: Silver Kill-Events Telemetry
  # Chỉ tạo nếu có tọa độ thật — không mock dữ liệu nếu schema không có
  cat("[INFO] Trích xuất Bảng 4: Silver Kill-Events Telemetry...\n")
  silver_kill_events_df <- data.frame(
    match_id = character(), player_id = character(),
    killer_x = numeric(), killer_y = numeric(),
    victim_x = numeric(), victim_y = numeric(),
    distance_euclidean = numeric(), ingest_time = character(),
    stringsAsFactors = FALSE
  )
  # Nếu schema match_deaths có tọa độ (killer_position_x, killer_position_y thô từ Rust)
  # thì Bronze Parquet có thể chứa các cột này — kiểm tra và trích xuất
  # Hiện tại Rust Arrow schema chỉ lưu 19 trường chuẩn, không có raw coordinates
  # TODO: Mở rộng Arrow schema để giữ killer_position_x/y khi Op=OpKillEvent

  # 9. Ghi các file Silver Parquet ra output_dir (Rust sẽ upload)
  silver_base_dir <- file.path(output_dir, "silver")
  dir.create(file.path(silver_base_dir, "players"),      recursive = TRUE, showWarnings = FALSE)
  dir.create(file.path(silver_base_dir, "matches"),      recursive = TRUE, showWarnings = FALSE)
  dir.create(file.path(silver_base_dir, "player-match"), recursive = TRUE, showWarnings = FALSE)
  dir.create(file.path(silver_base_dir, "kill-events"),  recursive = TRUE, showWarnings = FALSE)

  players_out      <- file.path(silver_base_dir, "players",      sprintf("players_%s.parquet", batch_id))
  matches_out      <- file.path(silver_base_dir, "matches",      sprintf("matches_%s.parquet", batch_id))
  player_match_out <- file.path(silver_base_dir, "player-match", sprintf("player_match_%s.parquet", batch_id))
  kill_events_out  <- file.path(silver_base_dir, "kill-events",  sprintf("kill_events_%s.parquet", batch_id))

  if (requireNamespace("arrow", quietly = TRUE)) {
    arrow::write_parquet(silver_players_df,     players_out,      compression = "zstd")
    arrow::write_parquet(silver_matches_df,     matches_out,      compression = "zstd")
    arrow::write_parquet(silver_player_match_df, player_match_out, compression = "zstd")
    arrow::write_parquet(silver_kill_events_df, kill_events_out,  compression = "zstd")
  } else {
    # Fallback CSV nếu môi trường chưa có arrow C++ bindings
    write.csv(silver_players_df,     paste0(players_out, ".csv"),      row.names = FALSE)
    write.csv(silver_matches_df,     paste0(matches_out, ".csv"),      row.names = FALSE)
    write.csv(silver_player_match_df, paste0(player_match_out, ".csv"), row.names = FALSE)
    write.csv(silver_kill_events_df, paste0(kill_events_out, ".csv"),  row.names = FALSE)
  }

  cat(sprintf("[SUCCESS] Đã ghi Silver entities: %s, %s, %s\n",
              players_out, matches_out, player_match_out))

  # 10. Data Quality Summary Report JSON
  summary_report <- list(
    batch_id           = batch_id,
    initial_records    = initial_rows,
    unique_records     = dedup_rows,
    player_match_rows  = nrow(silver_player_match_df),
    players_count      = nrow(silver_players_df),
    matches_count      = nrow(silver_matches_df),
    processed_timestamp = format(Sys.time(), "%Y-%m-%dT%H:%M:%SZ")
  )

  summary_json <- jsonlite::toJSON(summary_report, auto_unbox = TRUE, pretty = TRUE)
  cat(sprintf("[QUALITY REPORT]\n%s\n", summary_json))

  return(summary_report)
}

# ===========================================================================
# aggregate_player_match: Group kill events + match summaries thành player-match grain
# ===========================================================================
# Input : Bronze DataFrame (mỗi dòng = 1 event, kills=1 nếu là kill event)
# Output: DataFrame với 1 dòng = 1 player trong 1 trận
#   kills            = SUM(kills) theo (match_id, player_id) — tổng kills trong trận
#   damage_dealt     = SUM(damage_dealt) — tổng damage (0 nếu là kill-event schema)
#   headshot_kills   = SUM(headshot_kills) — tổng headshot
#   survival_duration = MAX(survival_duration) — thời điểm kill cuối cùng hoặc survival time
#   walk_distance    = MAX(walk_distance) — vị trí xa nhất (proxy cho match_summary) hoặc
#                      SUM cho kill events (accumulate movement)
#   win_place_perc   = MIN(win_place_perc) — placement tốt nhất (placement nhỏ = rank cao)
# ===========================================================================
aggregate_player_match <- function(df) {
  # Kiểm tra cột bắt buộc
  required_cols <- c("match_id", "player_id", "kills")
  missing_cols <- setdiff(required_cols, colnames(df))
  if (length(missing_cols) > 0) {
    stop(sprintf("[ERROR] Bronze DataFrame thiếu cột bắt buộc: %s",
                 paste(missing_cols, collapse = ", ")))
  }

  # Đảm bảo các cột số tồn tại với giá trị mặc định 0 nếu thiếu
  numeric_cols <- c("damage_dealt", "headshot_kills", "walk_distance",
                    "ride_distance", "swim_distance", "survival_duration", "win_place_perc")
  for (col in numeric_cols) {
    if (!(col %in% colnames(df))) {
      df[[col]] <- 0.0
    }
  }

  # Group by (match_id, player_id) và aggregate
  # Dùng aggregate() với cbind để xử lý nhiều cột cùng lúc
  agg <- aggregate(
    cbind(kills, damage_dealt, headshot_kills, walk_distance,
          ride_distance, swim_distance, survival_duration) ~ match_id + player_id,
    data = df,
    FUN = function(x) {
      # kills/damage/headshots: SUM (tổng tích lũy)
      # Trả về list để aggregate cbind() dùng correctly
      sum(x)
    }
  )

  # win_place_perc: lấy giá trị cuối cùng (thường là 1 giá trị duy nhất cho match_summary,
  # hoặc giá trị cao nhất cho kill events — 1/placement nhỏ nhất = rank tốt nhất)
  wp_agg <- aggregate(
    win_place_perc ~ match_id + player_id,
    data = df,
    FUN = function(x) {
      valid <- x[!is.na(x) & x > 0]
      if (length(valid) == 0) return(0.0)
      max(valid)  # Giá trị lớn nhất = rank tốt nhất (match_summary: 1.0 = top 1)
    }
  )

  # survival_duration: aggregate riêng để dùng MAX (thời điểm kill cuối cùng trong trận)
  surv_agg <- aggregate(
    survival_duration ~ match_id + player_id,
    data = df,
    FUN = max
  )

  # Merge kết quả
  result <- merge(agg, wp_agg,   by = c("match_id", "player_id"), suffixes = c("", "_wp"))
  # Ưu tiên giá trị survival_duration từ surv_agg (MAX) thay vì SUM từ agg
  result$survival_duration <- NULL
  result <- merge(result, surv_agg, by = c("match_id", "player_id"))

  cat(sprintf("[INFO] aggregate_player_match: %d rows -> %d player-match records\n",
              nrow(df), nrow(result)))

  return(result)
}
