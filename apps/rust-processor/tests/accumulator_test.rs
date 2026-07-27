use rust_processor::domain::{EventEnvelope, PlayerStatPayload, SourceMetadata};
use rust_processor::ingest::{BatchAccumulator, BatchAccumulatorConfig, ConsumedMessage};
use std::time::Duration;

fn create_mock_message(partition: i32, offset: i64) -> ConsumedMessage {
    ConsumedMessage {
        envelope: EventEnvelope {
            schema_version: "1.0".to_string(),
            event_id: format!("ev-{}-{}", partition, offset),
            op: "data.player_stat.match_summary".to_string(),
            event_time: None,
            ingest_time: "2026-07-28T02:00:00Z".to_string(),
            match_id: "match-100".to_string(),
            player_id: format!("player-{}", offset),
            source: SourceMetadata {
                provider: "kaggle".to_string(),
                dataset_id: "pubg".to_string(),
                source_file: "train_V2.csv".to_string(),
                record_index: offset,
            },
            payload: PlayerStatPayload {
                kills: 2,
                damage_dealt: 210.0,
                headshot_kills: 1,
                walk_distance: 500.0,
                ride_distance: 0.0,
                swim_distance: 0.0,
                survival_duration: 600.0,
                win_place_perc: Some(0.5),
            },
        },
        topic: "pubg.v1.player-stat.raw".to_string(),
        partition,
        offset,
        key: Some("match-100".to_string()),
    }
}

// Test_accumulator_record_count_trigger kiểm tra tự động trigger flush khi đạt Record Count
#[test]
fn test_accumulator_record_count_trigger() {
    let cfg = BatchAccumulatorConfig {
        max_records: 2, // Flush khi đạt 2 bản ghi
        max_bytes: 100000,
        flush_interval: Duration::from_secs(60),
    };
    let mut accum = BatchAccumulator::new(cfg);

    let msg1 = create_mock_message(0, 10);
    let msg2 = create_mock_message(0, 11);

    let res1 = accum.push(msg1);
    assert!(res1.is_none(), "Tin nhắn 1 chưa đạt ngưỡng count=2");

    let res2 = accum.push(msg2);
    assert!(res2.is_some(), "Tin nhắn 2 đạt ngưỡng count=2 phải trigger flush");

    let batch = res2.unwrap();
    assert_eq!(batch.record_count, 2);
    assert_eq!(batch.partition_offsets.get(&0), Some(&(10, 11)));
}

// Test_accumulator_byte_size_trigger kiểm tra tự động trigger flush khi đạt Byte Size
#[test]
fn test_accumulator_byte_size_trigger() {
    let cfg = BatchAccumulatorConfig {
        max_records: 1000,
        max_bytes: 150, // Ngưỡng byte nhỏ 150 bytes
        flush_interval: Duration::from_secs(60),
    };
    let mut accum = BatchAccumulator::new(cfg);

    let msg1 = create_mock_message(0, 10);

    let res1 = accum.push(msg1);
    assert!(res1.is_some(), "Kích thước byte tin nhắn (~280 bytes) vượt ngưỡng 150 bytes phải trigger flush");

    let batch = res1.unwrap();
    assert_eq!(batch.record_count, 1);
}

// Test_accumulator_partition_offset_tracking kiểm tra theo dõi min_offset và max_offset theo từng partition
#[test]
fn test_accumulator_partition_offset_tracking() {
    let cfg = BatchAccumulatorConfig {
        max_records: 10,
        max_bytes: 100000,
        flush_interval: Duration::from_secs(60),
    };
    let mut accum = BatchAccumulator::new(cfg);

    // Push tin nhắn từ 2 Partition khác nhau: P0 (offset 5 -> 15) và P1 (offset 100 -> 105)
    accum.push(create_mock_message(0, 5));
    accum.push(create_mock_message(0, 15));
    accum.push(create_mock_message(1, 100));
    accum.push(create_mock_message(1, 105));

    let batch = accum.flush().unwrap();
    assert_eq!(batch.record_count, 4);

    let p0_offsets = batch.partition_offsets.get(&0).unwrap();
    assert_eq!(*p0_offsets, (5, 15), "Partition 0 min=5, max=15");

    let p1_offsets = batch.partition_offsets.get(&1).unwrap();
    assert_eq!(*p1_offsets, (100, 105), "Partition 1 min=100, max=105");
}
