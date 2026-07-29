# R Gold Feature Engine Module - Trích xuất ma trận đặc trưng ML Anti-Cheat
# ==============================================================================
# Trích xuất 5 đặc trưng chuẩn telemetry & player-match:
#   1. kills                                  : Tổng số mạng tiêu diệt trong trận
#   2. minimum_kill_interval_seconds          : Thời gian ngắn nhất giữa 2 kill liên tiếp (s)
#   3. median_kill_distance_coordinate_units : Khoảng cách Euclidean trung vị tới nạn nhân
#   4. short_kill_interval_count              : Số khoảng thời gian kill <= 10s (burst kills)
#   5. unique_weapons_used                    : Số loại vũ khí đã dùng

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

  manifest <- read_manifest(manifest_path)
  batch_id <- manifest$batch_id

  silver_ke_path <- file.path(output_dir, "silver", "kill-events",  sprintf("kill_events_%s.parquet", batch_id))
  silver_pm_path <- file.path(output_dir, "silver", "player-match", sprintf("player_match_%s.parquet", batch_id))

  cat(sprintf("[INFO] Đọc Silver kill_events từ: %s\n", silver_ke_path))
  cat(sprintf("[INFO] Đọc Silver player_match từ: %s\n", silver_pm_path))

  if (!file.exists(silver_ke_path) && !file.exists(paste0(silver_ke_path, ".csv"))) {
    stop(sprintf("[FAIL-CLOSE] Silver kill-events file không tồn tại tại: %s", silver_ke_path))
  }

  df_ke <- if (file.exists(silver_ke_path)) read_bronze_parquet(silver_ke_path) else read.csv(paste0(silver_ke_path, ".csv"))
  df_pm <- if (file.exists(silver_pm_path)) read_bronze_parquet(silver_pm_path) else read.csv(paste0(silver_pm_path, ".csv"))

  cat(sprintf("[INFO] Đọc Silver: %d kill events, %d player-match records\n", nrow(df_ke), nrow(df_pm)))

  if (nrow(df_pm) == 0) {
    gold_df <- data.frame(
      match_id = character(), player_id = character(), kills = integer(),
      minimum_kill_interval_seconds = numeric(),
      median_kill_distance_coordinate_units = numeric(),
      short_kill_interval_count = integer(),
      unique_weapons_used = integer(),
      feature_version = character(), created_at = character(),
      stringsAsFactors = FALSE
    )
    return(gold_df)
  }

  # 1. Euclidean distance
  df_ke$kill_distance_coords <- ifelse(
    !is.na(df_ke$killer_position_x) & !is.na(df_ke$victim_position_x) &
    !is.na(df_ke$killer_position_y) & !is.na(df_ke$victim_position_y),
    sqrt((df_ke$killer_position_x - df_ke$victim_position_x)^2 +
         (df_ke$killer_position_y - df_ke$victim_position_y)^2),
    NA_real_
  )

  # 2. Kill intervals
  valid_ke <- df_ke[!is.na(df_ke$killer_name) & df_ke$killer_name != "", ]

  if (nrow(valid_ke) > 0 && "event_time_seconds" %in% colnames(valid_ke)) {
    valid_ke <- valid_ke[order(valid_ke$match_id, valid_ke$killer_name, valid_ke$event_time_seconds), ]
  }

  dist_agg <- aggregate(kill_distance_coords ~ match_id + killer_name, data = valid_ke,
                        FUN = function(d) {
                          valid_d <- d[!is.na(d)]
                          if (length(valid_d) == 0) NA_real_ else median(valid_d)
                        })
  colnames(dist_agg) <- c("match_id", "player_id", "median_kill_distance_coordinate_units")

  interval_list <- list()
  if (nrow(valid_ke) > 0 && "event_time_seconds" %in% colnames(valid_ke)) {
    keys <- paste(valid_ke$match_id, valid_ke$killer_name, sep = "::")
    unique_keys <- unique(keys)

    for (k in unique_keys) {
      sub <- valid_ke[keys == k, ]
      times <- sub$event_time_seconds[!is.na(sub$event_time_seconds)]

      min_interval <- NA_real_
      burst_count  <- 0L

      if (length(times) > 1) {
        diffs <- diff(times)
        diffs <- diffs[diffs >= 0]
        if (length(diffs) > 0) {
          min_interval <- min(diffs)
          burst_count  <- as.integer(sum(diffs <= 10.0))
        }
      }

      parts <- strsplit(k, "::")[[1]]
      interval_list[[length(interval_list) + 1]] <- data.frame(
        match_id                      = parts[1],
        player_id                     = parts[2],
        minimum_kill_interval_seconds = min_interval,
        short_kill_interval_count     = burst_count,
        stringsAsFactors              = FALSE
      )
    }
  }

  interval_df <- if (length(interval_list) > 0) do.call(rbind, interval_list) else data.frame(
    match_id = character(), player_id = character(),
    minimum_kill_interval_seconds = numeric(), short_kill_interval_count = integer(),
    stringsAsFactors = FALSE
  )

  gold_df <- merge(df_pm, dist_agg,    by = c("match_id", "player_id"), all.x = TRUE)
  gold_df <- merge(gold_df, interval_df, by = c("match_id", "player_id"), all.x = TRUE)

  # Fill default numerical values cho features mà không bị bias
  gold_df$minimum_kill_interval_seconds[is.na(gold_df$minimum_kill_interval_seconds)]                   <- 999.0
  gold_df$median_kill_distance_coordinate_units[is.na(gold_df$median_kill_distance_coordinate_units)] <- 0.0
  gold_df$short_kill_interval_count[is.na(gold_df$short_kill_interval_count)]                         <- 0L
  gold_df$unique_weapons_used[is.na(gold_df$unique_weapons_used)]                                       <- 0L

  gold_df$feature_version <- "kill-event-player-match-v1"
  gold_df$created_at      <- format(Sys.time(), "%Y-%m-%dT%H:%M:%SZ")

  gold_final <- gold_df[, c(
    "match_id", "player_id", "kills",
    "minimum_kill_interval_seconds",
    "median_kill_distance_coordinate_units",
    "short_kill_interval_count",
    "unique_weapons_used",
    "feature_version", "created_at"
  )]

  gold_base_dir <- file.path(output_dir, "gold", "player-match-features")
  dir.create(gold_base_dir, recursive = TRUE, showWarnings = FALSE)
  gold_out <- file.path(gold_base_dir, sprintf("features_%s.parquet", batch_id))

  if (requireNamespace("arrow", quietly = TRUE)) {
    arrow::write_parquet(gold_final, gold_out, compression = "zstd")
  } else {
    write.csv(gold_final, paste0(gold_out, ".csv"), row.names = FALSE)
    gold_out <- paste0(gold_out, ".csv")
  }

  cat(sprintf("[SUCCESS] Ghi Gold Feature Store: %s (%d player-match features)\n",
              gold_out, nrow(gold_final)))

  return(gold_final)
}
