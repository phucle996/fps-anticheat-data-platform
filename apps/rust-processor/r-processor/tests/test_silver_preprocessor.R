# Test Script cho R Silver Preprocessor Module
# ==============================================================================

source("R/config.R")
source("R/manifest_reader.R")
source("R/storage.R")
source("R/silver_preprocessor.R")

cat("[TEST] Bắt đầu chạy kịch bản kiểm thử cho R Silver Preprocessor...\n")

# Tạo 1 file manifest giả lập để test
test_manifest_file <- tempfile(fileext = ".json")
test_manifest_content <- list(
  batch_id = "test_batch_silver_999",
  source_topic = "pubg.v1.player-stat.raw",
  partition_offsets = list("0" = list(min_offset = 0, max_offset = 10)),
  total_records_read = 3,
  valid_records_count = 3,
  invalid_records_count = 0,
  duplicate_records_count = 0,
  data_object_path = "bronze/player-stat/year=2026/month=07/day=28/pubg_player_stat_mock.parquet",
  checksum_sha256 = "8c53efdd7db58c4835214fac671639130cadcbffd533092751a5c9864e3963fb",
  processing_timestamp = "2026-07-28T04:19:00Z"
)

write(jsonlite::toJSON(test_manifest_content, auto_unbox = TRUE, pretty = TRUE), test_manifest_file)

# Kích hoạt hàm tiền xử lý Silver Entities
report <- process_silver_entities(test_manifest_file)

# Kiểm tra khẳng định (Assertions)
stopifnot(report$batch_id == "test_batch_silver_999")
stopifnot(report$unique_records == 2) # Do có 1 record trùng event_id bị loại
stopifnot(report$players_count == 2)  # player_A và player_B
stopifnot(report$matches_count == 1)  # match_100

cat("[TEST SUCCESS] Tất cả các kiểm thử cho R Silver Preprocessor đã PASS 100%!\n")
