use rust_processor::storage::{BatchManifest, MinioWriter, PartitionOffsetMetadata};
use std::collections::HashMap;

// Test_batch_manifest_serialization kiểm tra mã hóa và giải mã JSON cho BatchManifest audit log
#[test]
fn test_batch_manifest_serialization() {
    let mut partition_offsets = HashMap::new();
    partition_offsets.insert(
        0,
        PartitionOffsetMetadata {
            min_offset: 100,
            max_offset: 250,
        },
    );

    let manifest = BatchManifest {
        batch_id: "batch-test-999".to_string(),
        source_topic: "pubg.v1.player-stat.raw".to_string(),
        partition_offsets,
        total_records_read: 151,
        valid_records_count: 150,
        invalid_records_count: 1,
        duplicate_records_count: 0,
        data_object_path:
            "bronze/player-stat/year=2026/month=07/day=28/pubg_player_stat_batch-test-999.parquet"
                .to_string(),
        checksum_sha256: "8c53efdd7db58c4835214fac671639130cadcbffd533092751a5c9864e3963fb"
            .to_string(),
        processing_timestamp: "2026-07-28T02:40:00Z".to_string(),
    };

    let json_str = serde_json::to_string(&manifest).expect("Mã hóa JSON Manifest phải thành công");
    let decoded: BatchManifest =
        serde_json::from_str(&json_str).expect("Giải mã JSON Manifest phải thành công");

    assert_eq!(decoded.batch_id, "batch-test-999");
    assert_eq!(decoded.valid_records_count, 150);
    assert_eq!(decoded.partition_offsets.get(&0).unwrap().max_offset, 250);
}

// Test_generate_manifest_path_hive kiểm tra sinh đúng đường dẫn Hive Manifest
#[test]
fn test_generate_manifest_path_hive() {
    let path = MinioWriter::generate_manifest_path("batch-test-999", "2026-07-28T02:40:00Z");
    assert_eq!(
        path, "manifests/year=2026/month=07/day=28/manifest_batch-test-999.json",
        "Đường dẫn Manifest phải chuẩn định dạng Hive Partitioning"
    );
}
