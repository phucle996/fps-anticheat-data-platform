use rust_processor::storage::MinioWriter;

// Test_generate_bronze_path_hive_format kiểm tra sinh đúng cấu trúc đường dẫn Hive Partitioning
#[test]
fn test_generate_bronze_path_hive_format() {
    let batch_id = "batch-abc12345";
    let ingest_time = "2026-07-28T02:30:00Z";

    let path = MinioWriter::generate_bronze_path(batch_id, ingest_time);
    assert_eq!(
        path,
        "bronze/player-stat/year=2026/month=07/day=28/pubg_player_stat_batch-abc12345.parquet",
        "Cấu trúc đường dẫn Bronze Parquet phải tuân thủ chuẩn Hive Partitioning"
    );
}

// Test_generate_invalid_path_hive_format kiểm tra sinh đúng cấu trúc đường dẫn lưu bản ghi vi phạm
#[test]
fn test_generate_invalid_path_hive_format() {
    let batch_id = "batch-err999";
    let ingest_time = "2026-07-28T02:30:00Z";

    let path = MinioWriter::generate_invalid_path(batch_id, ingest_time);
    assert_eq!(
        path,
        "bronze/invalid/year=2026/month=07/day=28/pubg_invalid_batch-err999.json",
        "Cấu trúc đường dẫn Invalid Records phải tuân thủ chuẩn Hive Partitioning"
    );
}

// Test_compute_sha256_checksum kiểm tra tính toán mã SHA-256 checksum chính xác cho mảng bytes
#[test]
fn test_compute_sha256_checksum() {
    let sample_bytes = b"hello anticheat data lake";
    let checksum = MinioWriter::compute_sha256(sample_bytes);

    assert_eq!(checksum.len(), 64, "Mã băm SHA-256 phải đúng 64 ký tự Hex");
    assert_eq!(
        checksum,
        "8c53efdd7db58c4835214fac671639130cadcbffd533092751a5c9864e3963fb"
    );
}
