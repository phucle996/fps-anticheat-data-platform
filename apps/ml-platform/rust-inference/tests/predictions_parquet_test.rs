use rust_inference::storage::{PredictionParquetWriter, PredictionRecord};
use std::fs;

#[test]
fn test_write_predictions_json_stream() {
    let temp_file = std::env::temp_dir().join("test_predictions.parquet.json");

    let records = vec![
        PredictionRecord {
            match_id: "match_101".to_string(),
            player_id: "player_X".to_string(),
            risk_score: 0.95,
            risk_level: "CRITICAL".to_string(),
            model_version: "v1".to_string(),
            timestamp: "2026-07-28T05:00:00Z".to_string(),
        },
        PredictionRecord {
            match_id: "match_101".to_string(),
            player_id: "player_Y".to_string(),
            risk_score: 0.12,
            risk_level: "LOW".to_string(),
            model_version: "v1".to_string(),
            timestamp: "2026-07-28T05:00:00Z".to_string(),
        },
    ];

    let result = PredictionParquetWriter::write_predictions_json_stream(&records, &temp_file);
    assert!(result.is_ok());

    let content = fs::read_to_string(&temp_file).unwrap();
    assert!(content.contains("match_101"));
    assert!(content.contains("player_X"));
    assert!(content.contains("CRITICAL"));

    let _ = fs::remove_file(temp_file);
}
