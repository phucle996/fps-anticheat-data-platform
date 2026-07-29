# R Silver Preprocessor Module - Phân tách 2 bảng Silver Entities chuẩn hóa
# ==============================================================================
# Bảng 1: silver_kill_events  (Grain: 1 row = 1 kill event, giữ telemetry raw)
# Bảng 2: silver_player_match (Grain: 1 row = 1 player trong 1 match, aggregate full run)

suppressPackageStartupMessages({
  if (!requireNamespace("jsonlite", quietly = TRUE)) {
    install.packages("jsonlite", repos = "https://cloud.r-project.org")
  }
  library(jsonlite)
})

source("R/manifest_reader.R")
source("R/storage.R")

safe_min <- function(x) {
  valid <- x[!is.na(x) & !is.nan(x) & !is.infinite(x)]
  if (length(valid) == 0) return(NA_real_)
  min(valid)
}

safe_max <- function(x) {
  valid <- x[!is.na(x) & !is.nan(x) & !is.infinite(x)]
  if (length(valid) == 0) return(NA_real_)
  max(valid)
}

process_silver_entities <- function(manifest_path, bronze_path, output_dir) {
  cat(sprintf("[INFO] Silver Preprocessor bắt đầu. Manifest: %s\n", manifest_path))

  # 1. Đọc Manifest JSON (Fail-Close: dừng ngay nếu không tìm thấy)
  manifest <- read_manifest(manifest_path)
  batch_id <- manifest$batch_id

  # 2. Đọc Bronze Parquet từ local path đã download
  cat(sprintf("[INFO] Đọc Bronze Parquet từ local path: %s\n", bronze_path))
  df <- read_bronze_parquet(bronze_path)

  if (is.null(df) || nrow(df) == 0) {
    stop(sprintf("[FAIL-CLOSE] Bronze Parquet rỗng hoặc không đọc được tại: %s", bronze_path))
  }

  cat(sprintf("[INFO] Đọc thành công %d records từ Bronze Parquet\n", nrow(df)))

  # 3. Deduplication theo `event_id`
  initial_rows <- nrow(df)
  if ("event_id" %in% colnames(df)) {
    df <- df[!duplicated(df$event_id), ]
  }
  dedup_rows <- nrow(df)
  cat(sprintf("[INFO] Dedup event_id: %d -> %d bản ghi độc nhất\n", initial_rows, dedup_rows))

  # ===========================================================================
  # BẢNG 1: silver_kill_events (Grain: 1 dòng = 1 kill event)
  # ===========================================================================
  cat("[INFO] Trích xuất Bảng 1: silver_kill_events...\n")

  # Trích xuất các cột telemetry thực sự nếu có
  get_col <- function(col_name, default_val = NA) {
    if (col_name %in% colnames(df)) df[[col_name]] else rep(default_val, nrow(df))
  }

  silver_kill_events_df <- data.frame(
    event_id           = get_col("event_id", ""),
    match_id           = get_col("match_id", ""),
    killer_name        = get_col("player_id", ""),
    victim_name        = get_col("victim_name", NA_character_),
    killer_placement   = get_col("killer_placement", NA_integer_),
    victim_placement   = get_col("victim_placement", NA_integer_),
    killer_position_x  = get_col("killer_position_x", NA_real_),
    killer_position_y  = get_col("killer_position_y", NA_real_),
    victim_position_x  = get_col("victim_position_x", NA_real_),
    victim_position_y  = get_col("victim_position_y", NA_real_),
    event_time_seconds = get_col("event_time_seconds", NA_real_),
    weapon             = get_col("weapon", NA_character_),
    ingest_time        = get_col("ingest_time", format(Sys.time(), "%Y-%m-%dT%H:%M:%SZ")),
    stringsAsFactors   = FALSE
  )

  # ===========================================================================
  # BẢNG 2: silver_player_match (Grain: 1 dòng = 1 player trong 1 match)
  # ===========================================================================
  cat("[INFO] Aggregate Bảng 2: silver_player_match bằng group_by(match_id, killer_name)...\n")

  # Lọc các bản ghi có killer_name hợp lệ
  valid_killers <- silver_kill_events_df[!is.na(silver_kill_events_df$killer_name) &
                                          silver_kill_events_df$killer_name != "", ]

  if (nrow(valid_killers) == 0) {
    # Fallback nếu không có killer_name nào valid
    silver_player_match_df <- data.frame(
      match_id = character(), player_id = character(), kills = integer(),
      first_kill_time_seconds = numeric(), last_kill_time_seconds = numeric(),
      unique_weapons_used = integer(), killer_placement = integer(),
      stringsAsFactors = FALSE
    )
  } else {
    # Aggregate theo (match_id, killer_name)
    kills_agg <- aggregate(event_id ~ match_id + killer_name, data = valid_killers, FUN = length)
    colnames(kills_agg) <- c("match_id", "player_id", "kills")

    time_min_agg <- aggregate(event_time_seconds ~ match_id + killer_name, data = valid_killers, FUN = safe_min)
    colnames(time_min_agg) <- c("match_id", "player_id", "first_kill_time_seconds")

    time_max_agg <- aggregate(event_time_seconds ~ match_id + killer_name, data = valid_killers, FUN = safe_max)
    colnames(time_max_agg) <- c("match_id", "player_id", "last_kill_time_seconds")

    weapons_agg <- aggregate(weapon ~ match_id + killer_name, data = valid_killers,
                             FUN = function(w) length(unique(w[!is.na(w)])))
    colnames(weapons_agg) <- c("match_id", "player_id", "unique_weapons_used")

    place_agg <- aggregate(killer_placement ~ match_id + killer_name, data = valid_killers,
                           FUN = function(p) {
                             valid_p <- p[!is.na(p)]
                             if (length(valid_p) == 0) NA_integer_ else valid_p[1]
                           })
    colnames(place_agg) <- c("match_id", "player_id", "killer_placement")

    # Merge các aggregates
    res <- merge(kills_agg, time_min_agg, by = c("match_id", "player_id"), all.x = TRUE)
    res <- merge(res, time_max_agg,       by = c("match_id", "player_id"), all.x = TRUE)
    res <- merge(res, weapons_agg,        by = c("match_id", "player_id"), all.x = TRUE)
    res <- merge(res, place_agg,          by = c("match_id", "player_id"), all.x = TRUE)

    silver_player_match_df <- res
  }

  cat(sprintf("[INFO] Grain check: %d kill events -> %d unique player-match records\n",
              nrow(silver_kill_events_df), nrow(silver_player_match_df)))

  # 4. Ghi output Parquet
  silver_base_dir <- file.path(output_dir, "silver")
  dir.create(file.path(silver_base_dir, "kill-events"),  recursive = TRUE, showWarnings = FALSE)
  dir.create(file.path(silver_base_dir, "player-match"), recursive = TRUE, showWarnings = FALSE)

  kill_events_out  <- file.path(silver_base_dir, "kill-events",  sprintf("kill_events_%s.parquet", batch_id))
  player_match_out <- file.path(silver_base_dir, "player-match", sprintf("player_match_%s.parquet", batch_id))

  if (requireNamespace("arrow", quietly = TRUE)) {
    arrow::write_parquet(silver_kill_events_df, kill_events_out,  compression = "zstd")
    arrow::write_parquet(silver_player_match_df, player_match_out, compression = "zstd")
  } else {
    write.csv(silver_kill_events_df,  paste0(kill_events_out, ".csv"),  row.names = FALSE)
    write.csv(silver_player_match_df, paste0(player_match_out, ".csv"), row.names = FALSE)
  }

  cat(sprintf("[SUCCESS] Đã ghi Silver entities: %s, %s\n", kill_events_out, player_match_out))

  summary_report <- list(
    batch_id           = batch_id,
    initial_records    = initial_rows,
    unique_records     = dedup_rows,
    kill_event_rows    = nrow(silver_kill_events_df),
    player_match_rows  = nrow(silver_player_match_df),
    processed_timestamp = format(Sys.time(), "%Y-%m-%dT%H:%M:%SZ")
  )

  return(summary_report)
}
