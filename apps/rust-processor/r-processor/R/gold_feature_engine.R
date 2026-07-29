# R Gold Feature Engine Module - Trích xuất ma trận đặc trưng ML Anti-Cheat
# ==============================================================================
# Trích xuất 5 đặc trưng thật từ kill telemetry & player-match:
#   1. kills                                  : Tổng số mạng tiêu diệt trong trận
#   2. minimum_kill_interval_seconds          : Thời gian ngắn nhất giữa 2 kill liên tiếp (s)
#   3. median_kill_distance_coordinate_units : Khoảng cách Euclidean trung vị tới nạn nhân
#   4. kills_within_10_seconds                : Số cú kill dồn dập trong khoảng thời gian <= 10s
#   5. unique_weapons_used                    : Số loại vũ khí đã dùng
# KHÔNG tự tạo công thức proxy giả (vd: walk_distance * 0.4 + kills * 15)

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

  # Đọc Silver kill_events và player_match từ output_dir
  silver_ke_path <- file.path(output_dir, "silver", "kill-events",  sprintf("kill_events_%s.parquet", batch_id))
  silver_pm_path <- file.path(output_dir, "silver", "player-match", sprintf("player_match_%s.parquet", batch_id))

  cat(sprintf("[INFO] Đọc Silver kill_events từ: %s\n", silver_ke_path))
  cat(sprintf("[INFO] Đọc Silver player_match từ: %s\n", silver_pm_path))

  # Fail-Close check
  if (!file.exists(silver_ke_path) && !file.exists(paste0(silver_ke_path, ".csv"))) {
    stop(sprintf("[FAIL-CLOSE] Silver kill-events file không tồn tại tại: %s", silver_ke_path))
  }

  df_ke <- if (file.exists(silver_ke_path)) read_bronze_parquet(silver_ke_path) else read.csv(paste0(silver_ke_path, ".csv"))
  df_pm <- if (file.exists(silver_pm_path)) read_bronze_parquet(silver_pm_path) else read.csv(paste0(silver_pm_path, ".csv"))

  cat(sprintf("[INFO] Đọc Silver: %d kill events, %d player-match records\n", nrow(df_ke), nrow(df_pm)))

  # Nếu không có data -> trả về Gold empty dataframe
  if (nrow(df_pm) == 0) {
    gold_df <- data.frame(
      match_id = character(), player_id = character(), kills = integer(),
      minimum_kill_interval_seconds = numeric(),
      median_kill_distance_coordinate_units = numeric(),
      kills_within_10_seconds = integer(),
      unique_weapons_used = integer(),
      feature_version = character(), created_at = character(),
      stringsAsFactors = FALSE
    )
    return(gold_df)
  }

  # 1. Tính toán Euclidean distance cho từng kill event có đủ tọa độ killer & victim
  df_ke$kill_distance_coords <- ifelse(
    !is.na(df_ke$killer_position_x) & !is.na(df_ke$victim_position_x) &
    !is.na(df_ke$killer_position_y) & !is.na(df_ke$victim_position_y),
    sqrt((df_ke$killer_position_x - df_ke$victim_position_x)^2 +
         (df_ke$killer_position_y - df_ke$victim_position_y)^2),
    NA_real_
  )

  # 2. Tính toán kill intervals (thời gian giữa 2 cú kill liên tiếp của cùng 1 killer)
  valid_ke <- df_ke[!is.na(df_ke$killer_name) & df_ke$killer_name != "", ]

  # Sắp xếp theo match_id, killer_name, event_time_seconds
  if (nrow(valid_ke) > 0 && "event_time_seconds" %in% colnames(valid_ke)) {
    valid_ke <- valid_ke[order(valid_ke$match_id, valid_ke$killer_name, valid_ke$event_time_seconds), ]
  }

  # Aggregate median kill distance per player-match
  dist_agg <- aggregate(kill_distance_coords ~ match_id + killer_name, data = valid_ke,
                        FUN = function(d) {
                          valid_d <- d[!is.na(d)]
                          if (length(valid_d) == 0) 0.0 else median(valid_d)
                        })
  colnames(dist_agg) <- c("match_id", "player_id", "median_kill_distance_coordinate_units")

  # Aggregate min kill interval & burst kills <= 10s per player-match
  interval_list <- list()
  if (nrow(valid_ke) > 0 && "event_time_seconds" %in% colnames(valid_ke)) {
    # Group by match_id + killer_name để tính lag interval
    keys <- paste(valid_ke$match_id, valid_ke$killer_name, sep = "::")
    unique_keys <- unique(keys)

    for (k in unique_keys) {
      sub <- valid_ke[keys == k, ]
      times <- sub$event_time_seconds[!is.na(sub$event_time_seconds)]

      min_interval <- 9999.0
      burst_count  <- 0

      if (length(times) > 1) {
        diffs <- diff(times)
        diffs <- diffs[diffs >= 0]
        if (length(diffs) > 0) {
          min_interval <- min(diffs)
          burst_count  <- sum(diffs <= 10.0)
        }
      }

      if (min_interval == 9999.0) min_interval <- 0.0

      parts <- strsplit(k, "::")[[1]]
      interval_list[[length(interval_list) + 1]] <- data.frame(
        match_id                      = parts[1],
        player_id                     = parts[2],
        minimum_kill_interval_seconds = min_interval,
        kills_within_10_seconds       = burst_count,
        stringsAsFactors              = FALSE
      )
    }
  }

  interval_df <- if (length(interval_list) > 0) do.call(rbind, interval_list) else data.frame(
    match_id = character(), player_id = character(),
    minimum_kill_interval_seconds = numeric(), kills_within_10_seconds = integer(),
    stringsAsFactors = FALSE
  )

  # Merge features vào df_pm
  gold_df <- merge(df_pm, dist_agg,    by = c("match_id", "player_id"), all.x = TRUE)
  gold_df <- merge(gold_df, interval_df, by = c("match_id", "player_id"), all.x = TRUE)

  # Fill NAs
  gold_df$minimum_kill_interval_seconds[is.na(gold_df$minimum_kill_interval_seconds)]                   <- 0.0
  gold_df$median_kill_distance_coordinate_units[is.na(gold_df$median_kill_distance_coordinate_units)] <- 0.0
  gold_df$kills_within_10_seconds[is.na(gold_df$kills_within_10_seconds)]                               <- 0L
  gold_df$unique_weapons_used[is.na(gold_df$unique_weapons_used)]                                       <- 0L

  gold_df$feature_version <- "v1.0"
  gold_df$created_at      <- format(Sys.time(), "%Y-%m-%dT%H:%M:%SZ")

  # Select 5 feature contract cols
  gold_final <- gold_df[, c(
    "match_id", "player_id", "kills",
    "minimum_kill_interval_seconds",
    "median_kill_distance_coordinate_units",
    "kills_within_10_seconds",
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
