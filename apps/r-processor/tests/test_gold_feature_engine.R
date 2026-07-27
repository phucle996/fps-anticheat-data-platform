# Test Script cho R Gold Feature Engine Module
# ==============================================================================

source("R/config.R")
source("R/manifest_reader.R")
source("R/storage.R")
source("R/silver_preprocessor.R")
source("R/gold_feature_engine.R")

cat("[TEST] Bắt đầu chạy kịch bản kiểm thử cho R Gold Feature Engine...\n")

# Tạo 1 file manifest giả lập để test Gold Feature Engine
test_manifest_file <- tempfile(fileext = ".json")
test_manifest_content <- list(
  batch_id = "test_batch_gold_999",
  source_topic = "pubg.v1.player-stat.raw",
  partition_offsets = list("0" = list(min_offset = 0, max_offset = 10)),
  total_records_read = 3,
  valid_records_count = 3,
  invalid_records_count = 0,
  duplicate_records_count = 0,
  data_object_path = "bronze/player-stat/year=2026/month=07/day=28/pubg_player_stat_mock.parquet",
  checksum_sha256 = "8c53efdd7db58c4835214fac671639130cadcbffd533092751a5c9864e3963fb",
  processing_timestamp = "2026-07-28T04:48:00Z"
)

write(jsonlite::toJSON(test_manifest_content, auto_unbox = TRUE, pretty = TRUE), test_manifest_file)

# Kích hoạt trích xuất đặc trưng ML Gold
gold_df <- generate_gold_features(test_manifest_file)

# Kiểm tra khẳng định (Assertions)
stopifnot(nrow(gold_df) == 3)
stopifnot("total_distance" %in% colnames(gold_df))
stopifnot("headshot_ratio" %in% colnames(gold_df))
stopifnot("kills_per_minute" %in% colnames(gold_df))
stopifnot("damage_per_minute" %in% colnames(gold_df))
stopifnot("damage_per_kill" %in% colnames(gold_df))
stopifnot("movement_per_minute" %in% colnames(gold_df))
stopifnot("performance_versus_lobby" %in% colnames(gold_df))

# Kiểm tra an toàn toán học: Không có NA, Inf hoặc NaN
stopifnot(sum(is.na(gold_df$headshot_ratio)) == 0)
stopifnot(sum(is.infinite(gold_df$kills_per_minute)) == 0)
stopifnot(sum(is.nan(gold_df$damage_per_minute)) == 0)

cat("[TEST SUCCESS] Tất cả các kiểm thử cho R Gold Feature Engine đã PASS 100%!\n")
