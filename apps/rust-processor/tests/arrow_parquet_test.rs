use rust_processor::domain::{EventEnvelope, PlayerStatPayload, SourceMetadata};
use rust_processor::transform::{ArrowConverter, ParquetSerializer};
use std::fs;

fn create_sample_events() -> Vec<EventEnvelope> {
    vec![
        EventEnvelope {
            schema_version: "1.0".to_string(),
            event_id: "a6793109ae2d60d5f3576cf8cf5ef0c23996b63e376ea8d2d67b7315e316ac01".to_string(),
            op: "data.player_stat.match_summary".to_string(),
            event_time: Some("2026-07-28T02:00:00Z".to_string()),
            ingest_time: "2026-07-28T02:00:00Z".to_string(),
            match_id: "match-100".to_string(),
            player_id: "player-1".to_string(),
            source: SourceMetadata {
                provider: "kaggle".to_string(),
                dataset_id: "pubg".to_string(),
                source_file: "train_V2.csv".to_string(),
                record_index: 10,
            },
            payload: PlayerStatPayload {
                kills: 5,
                damage_dealt: 550.5,
                headshot_kills: 2,
                walk_distance: 1200.0,
                ride_distance: 0.0,
                swim_distance: 0.0,
                survival_duration: 900.0,
                win_place_perc: Some(0.85),
            },
        },
        EventEnvelope {
            schema_version: "1.0".to_string(),
            event_id: "b7793109ae2d60d5f3576cf8cf5ef0c23996b63e376ea8d2d67b7315e316ac02".to_string(),
            op: "data.player_stat.match_summary".to_string(),
            event_time: None,
            ingest_time: "2026-07-28T02:00:01Z".to_string(),
            match_id: "match-100".to_string(),
            player_id: "player-2".to_string(),
            source: SourceMetadata {
                provider: "kaggle".to_string(),
                dataset_id: "pubg".to_string(),
                source_file: "train_V2.csv".to_string(),
                record_index: 11,
            },
            payload: PlayerStatPayload {
                kills: 0,
                damage_dealt: 0.0,
                headshot_kills: 0,
                walk_distance: 150.0,
                ride_distance: 0.0,
                swim_distance: 0.0,
                survival_duration: 200.0,
                win_place_perc: None, // Test null value
            },
        },
    ]
}

// Test_arrow_schema_transformation kiểm tra tạo Arrow Schema và RecordBatch 19 cột
#[test]
fn test_arrow_schema_transformation() {
    let events = create_sample_events();
    let batch_res = ArrowConverter::events_to_record_batch(&events);
    assert!(batch_res.is_ok(), "Chuyển đổi Arrow RecordBatch phải thành công");

    let batch = batch_res.unwrap();
    assert_eq!(batch.num_rows(), 2);
    assert_eq!(batch.num_columns(), 19);

    // Kiểm tra tên các cột Arrow Schema
    let schema = batch.schema();
    assert_eq!(schema.field(0).name(), "event_id");
    assert_eq!(schema.field(11).name(), "kills");
    assert_eq!(schema.field(18).name(), "win_place_perc");
}

// Test_parquet_zstd_serialization_and_roundtrip kiểm tra nén Parquet Zstd, tạo local file và đọc lại round-trip
#[test]
fn test_parquet_zstd_serialization_and_roundtrip() {
    let events = create_sample_events();
    let batch = ArrowConverter::events_to_record_batch(&events).unwrap();

    // 1. Mã hóa RecordBatch thành byte Parquet Zstd
    let parquet_bytes_res = ParquetSerializer::record_batch_to_parquet_bytes(&batch);
    assert!(parquet_bytes_res.is_ok(), "Mã hóa Parquet Zstd phải thành công");

    let parquet_bytes = parquet_bytes_res.unwrap();
    assert!(!parquet_bytes.is_empty(), "Chuỗi byte Parquet không được rỗng");

    // 2. Ghi ra local file tạm để kiểm thử Parquet output file
    let temp_file_path = "/tmp/test_output_batch.parquet";
    fs::write(temp_file_path, &parquet_bytes).expect("Ghi file local test Parquet phải thành công");

    // 3. Đọc lại file Parquet vừa ghi và giải mã
    let file_bytes = fs::read(temp_file_path).expect("Đọc file local test Parquet phải thành công");
    let read_batches_res = ParquetSerializer::read_parquet_bytes(&file_bytes);
    assert!(read_batches_res.is_ok(), "Đọc lại Parquet round-trip phải thành công");

    let read_batches = read_batches_res.unwrap();
    assert_eq!(read_batches.len(), 1);
    assert_eq!(read_batches[0].num_rows(), 2);
    assert_eq!(read_batches[0].num_columns(), 19);

    // Dọn dẹp file tạm
    let _ = fs::remove_file(temp_file_path);
}
