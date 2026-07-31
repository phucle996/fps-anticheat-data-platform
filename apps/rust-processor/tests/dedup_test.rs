use rust_processor::domain::{AnyEnvelope, EventEnvelope, PlayerStatPayload, SourceMetadata};
use rust_processor::transform::EventDeduplicator;

fn create_event_with_id(id: &str) -> AnyEnvelope {
    AnyEnvelope::PlayerStat(EventEnvelope {
        schema_version: "1.0".to_string(),
        event_id: id.to_string(),
        op: "data.player_stat.match_summary".to_string(),
        event_time: None,
        ingest_time: "2026-07-28T02:00:00Z".to_string(),
        match_id: "match-100".to_string(),
        player_id: "player-1".to_string(),
        source: SourceMetadata {
            provider: "kaggle".to_string(),
            dataset_id: "pubg".to_string(),
            source_file: "train_V2.csv".to_string(),
            record_index: 1,
        },
        payload: PlayerStatPayload {
            kills: 3,
            damage_dealt: 300.0,
            headshot_kills: 1,
            walk_distance: 800.0,
            ride_distance: 0.0,
            swim_distance: 0.0,
            survival_duration: 500.0,
            win_place_perc: Some(0.6),
        },
    })
}

// Test_deduplicate_batch_with_duplicates kiểm tra lọc bỏ event_id bị trùng lặp trong batch
#[test]
fn test_deduplicate_batch_with_duplicates() {
    let e1 = create_event_with_id("id-aaa");
    let e2 = create_event_with_id("id-bbb");
    let e3 = create_event_with_id("id-aaa"); // Trùng với e1
    let e4 = create_event_with_id("id-ccc");

    let events = vec![e1, e2, e3, e4];
    let outcome = EventDeduplicator::deduplicate_batch(events);

    assert_eq!(
        outcome.unique_records.len(),
        3,
        "Kỳ vọng giữ lại 3 bản ghi duy nhất"
    );
    assert_eq!(
        outcome.duplicate_count, 1,
        "Kỳ vọng loại bỏ 1 bản ghi trùng"
    );
    assert_eq!(outcome.unique_records[0].event_id(), "id-aaa");
    assert_eq!(outcome.unique_records[1].event_id(), "id-bbb");
    assert_eq!(outcome.unique_records[2].event_id(), "id-ccc");
}

// Test_deduplicate_batch_unique kiểm tra khi tất cả các event_id đều là duy nhất
#[test]
fn test_deduplicate_batch_unique() {
    let e1 = create_event_with_id("id-111");
    let e2 = create_event_with_id("id-222");

    let events = vec![e1, e2];
    let outcome = EventDeduplicator::deduplicate_batch(events);

    assert_eq!(outcome.unique_records.len(), 2);
    assert_eq!(outcome.duplicate_count, 0);
}
