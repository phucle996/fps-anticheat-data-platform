# Test Script cho R EDA Analyzer Module
# ==============================================================================

source("R/storage.R")
source("R/eda_analyzer.R")

cat("[TEST] Bắt đầu chạy kịch bản kiểm thử cho R EDA Analyzer...\n")

# Kích hoạt tạo báo cáo EDA
report <- generate_eda_report(NULL)

# Kiểm tra khẳng định (Assertions)
stopifnot(report$total_records == 5)
stopifnot(report$unique_players == 5)
stopifnot(report$unique_matches == 3)
stopifnot("kills" %in% names(report$summary_stats))
stopifnot("headshot_ratio" %in% names(report$summary_stats))
stopifnot(length(report$selected_features_for_model) == 6)

cat("[TEST SUCCESS] Tất cả các kiểm thử cho R EDA Analyzer đã PASS 100%!\n")
