use rust_inference::inference::OnnxInferenceEngine;
use std::fs;

// Test_onnx_engine_prediction kiểm tra nạp model và tính toán Anomaly Risk Score
#[test]
fn test_onnx_engine_prediction() {
    let temp_dir = std::env::temp_dir().join("test_model_dir");
    let _ = fs::create_dir_all(&temp_dir);
    let model_file = temp_dir.join("model.onnx");
    let _ = fs::write(&model_file, b"ONNX_TEST_MODEL_BYTES");

    let engine = OnnxInferenceEngine::new(temp_dir.to_str().unwrap()).unwrap();

    // Features: [kills_pm, damage_pm, headshot_ratio, damage_per_kill, movement_pm, perf_vs_lobby]
    let features = [1.50, 140.0, 0.95, 120.0, 250.0, 800.0];
    let (score, level) = engine.predict(&features);

    assert!(score >= 0.80);
    assert_eq!(level, "CRITICAL");

    let _ = fs::remove_dir_all(temp_dir);
}
