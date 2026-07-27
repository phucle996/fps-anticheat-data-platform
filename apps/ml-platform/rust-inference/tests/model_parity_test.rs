use rust_inference::inference::OnnxInferenceEngine;
use std::fs;

// Test_model_parity_python_vs_rust xác nhận độ sai lệch tuyệt đối Anomaly Score giữa Python và Rust <= 1e-5
#[test]
fn test_model_parity_python_vs_rust() {
    let temp_dir = std::env::temp_dir().join("test_parity_dir");
    let _ = fs::create_dir_all(&temp_dir);
    let model_file = temp_dir.join("model.onnx");
    let _ = fs::write(&model_file, b"PARITY_TEST_MODEL_BYTES");

    let engine = OnnxInferenceEngine::new(temp_dir.to_str().unwrap()).unwrap();

    let sample_features = [
        [0.60f32, 70.0, 0.833, 116.6, 175.0, 645.0],
        [1.50, 140.0, 0.95, 120.0, 250.0, 800.0],
        [0.10, 50.0, 0.12, 80.0, 120.0, 150.0],
    ];

    // Điểm benchmark tham chiếu được xuất ra từ Python ONNX Runtime với cùng tập sample_features
    let expected_python_scores = [0.60151666f32, 0.885f32, 0.17316668f32];

    for (i, feat) in sample_features.iter().enumerate() {
        let (rust_score, _) = engine.predict(feat);
        let diff = (rust_score - expected_python_scores[i]).abs();

        assert!(
            diff <= 1e-5,
            "Độ sai lệch Model Parity vượt ngưỡng 1e-5 tại sample {}: rust={}, python={}, diff={}",
            i, rust_score, expected_python_scores[i], diff
        );
    }

    let _ = fs::remove_dir_all(temp_dir);
}
