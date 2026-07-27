use rust_processor::domain::{EventEnvelope, PlayerStatPayload, SourceMetadata};
use rust_processor::transform::EventValidator;

fn create_valid_event() -> EventEnvelope {
    EventEnvelope {
        schema_version: "1.0".to_string(),
        event_id: "a6793109ae2d60d5f3576cf8cf5ef0c23996b63e376ea8d2d67b7315e316ac01".to_string(),
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
            kills: 5,
            damage_dealt: 550.5,
            headshot_kills: 2,
            walk_distance: 1200.0,
            ride_distance: 0.0,
            swim_distance: 0.0,
            survival_duration: 900.0,
            win_place_perc: Some(0.85),
        },
    }
}

// Test_valid_event kiểm tra sự kiện hợp lệ qua 11 quy tắc validation
#[test]
fn test_valid_event() {
    let event = create_valid_event();
    let res = EventValidator::validate_event(&event);
    assert!(res.is_ok(), "Bản ghi chuẩn phải vượt qua 100% validation rules");
}

// Test_invalid_headshots_exceed_kills kiểm tra bắt lỗi headshots > kills
#[test]
fn test_invalid_headshots_exceed_kills() {
    let mut event = create_valid_event();
    event.payload.kills = 2;
    event.payload.headshot_kills = 5; // Headshots (5) > Kills (2)

    let res = EventValidator::validate_event(&event);
    assert!(res.is_err());
    let reasons = res.unwrap_err();
    assert!(reasons.iter().any(|r| r.contains("headshot_kills")));
}

// Test_negative_metrics kiểm tra bắt lỗi chỉ số âm
#[test]
fn test_negative_metrics() {
    let mut event = create_valid_event();
    event.payload.kills = -1;
    event.payload.damage_dealt = -50.0;
    event.payload.walk_distance = -10.0;

    let res = EventValidator::validate_event(&event);
    assert!(res.is_err());
    let reasons = res.unwrap_err();
    assert!(reasons.len() >= 3);
}

// Test_invalid_win_place_perc kiểm tra win_place_perc ngoài [0.0, 1.0]
#[test]
fn test_invalid_win_place_perc() {
    let mut event = create_valid_event();
    event.payload.win_place_perc = Some(1.5); // Out of range [0.0, 1.0]

    let res = EventValidator::validate_event(&event);
    assert!(res.is_err());
    let reasons = res.unwrap_err();
    assert!(reasons.iter().any(|r| r.contains("win_place_perc")));
}

// Test_invalid_event_id_hex kiểm tra event_id sai định dạng 64 hex
#[test]
fn test_invalid_event_id_hex() {
    let mut event = create_valid_event();
    event.event_id = "invalid-short-id".to_string(); // Sai độ dài hex

    let res = EventValidator::validate_event(&event);
    assert!(res.is_err());
    let reasons = res.unwrap_err();
    assert!(reasons.iter().any(|r| r.contains("event_id")));
}

// Test_validate_batch_mixed kiểm tra phân loại batch gồm cả bản ghi hợp lệ và vi phạm
#[test]
fn test_validate_batch_mixed() {
    let valid_event = create_valid_event();
    let mut invalid_event = create_valid_event();
    invalid_event.payload.win_place_perc = Some(2.5); // Vi phạm duy nhất win_place_perc

    let events = vec![valid_event, invalid_event];
    let outcome = EventValidator::validate_batch(events);

    assert_eq!(outcome.valid_count, 1);
    assert_eq!(outcome.invalid_count, 1);
    assert_eq!(outcome.invalid_records[0].reasons.len(), 1);
}
