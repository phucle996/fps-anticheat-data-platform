use rust_processor::domain::EventEnvelope;

// Test_deserialize_valid_event_json kiểm tra giải mã đúng JSON Event Envelope từ Go Ingestor
#[test]
fn test_deserialize_valid_event_json() {
    let raw_json = r#"{
        "schema_version": "1.0",
        "event_id": "a6793109ae2d60d5f3576cf8cf5ef0c23996b63e376ea8d2d67b7315e316ac01",
        "op": "data.player_stat.match_summary",
        "event_time": null,
        "ingest_time": "2026-07-28T02:00:00Z",
        "match_id": "match-100",
        "player_id": "player-1",
        "source": {
            "provider": "kaggle",
            "dataset_id": "daniboy370/pubg-finish-placement-prediction",
            "source_file": "train_V2.csv",
            "record_index": 1
        },
        "payload": {
            "kills": 5,
            "damage_dealt": 550.5,
            "headshot_kills": 2,
            "walk_distance": 1200.0,
            "ride_distance": 0.0,
            "swim_distance": 0.0,
            "survival_duration": 900.0,
            "win_place_perc": 0.85
        }
    }"#;

    let envelope: Result<EventEnvelope, _> = serde_json::from_str(raw_json);
    assert!(envelope.is_ok(), "Giải mã JSON EventEnvelope phải thành công");

    let env = envelope.unwrap();
    assert_eq!(env.match_id, "match-100");
    assert_eq!(env.player_id, "player-1");
    assert_eq!(env.payload.kills, 5);
    assert_eq!(env.payload.headshot_kills, 2);
    assert_eq!(env.payload.win_place_perc, Some(0.85));
}

// Test_deserialize_malformed_json_failure kiểm tra phát hiện và bắt lỗi khi JSON bị vi phạm cấu trúc
#[test]
fn test_deserialize_malformed_json_failure() {
    let malformed_json = r#"{ "match_id": "match-100", "broken_field": true }"#;

    let envelope: Result<EventEnvelope, _> = serde_json::from_str(malformed_json);
    assert!(envelope.is_err(), "Kỳ vọng giải mã Malformed JSON trả về lỗi");
}
