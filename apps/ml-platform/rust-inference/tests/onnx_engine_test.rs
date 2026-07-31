mod common;

use rust_inference::inference::OnnxInferenceEngine;

#[test]
fn test_onnx_engine_executes_model_and_validates_feature_contract() {
    let temp_dir = tempfile::tempdir().unwrap();
    common::write_test_bundle(temp_dir.path());
    let engine = OnnxInferenceEngine::new(temp_dir.path().to_str().unwrap()).unwrap();

    let (score, level) = engine.predict(&[8.0, 1.0, 1.0, 5.0, 4.0]).unwrap();
    assert!((score - 0.99310476).abs() <= 1e-5);
    assert_eq!(level, "CRITICAL");

    assert!(engine.predict(&[1.0, 2.0]).is_err());
    assert!(engine.predict(&[1.0, 2.0, f32::NAN, 4.0, 5.0]).is_err());
}

#[test]
fn test_invalid_candidate_does_not_replace_working_model() {
    let temp_dir = tempfile::tempdir().unwrap();
    common::write_test_bundle(temp_dir.path());
    let engine = OnnxInferenceEngine::new(temp_dir.path().to_str().unwrap()).unwrap();
    let version = engine.version();

    let invalid_dir = tempfile::tempdir().unwrap();
    std::fs::write(invalid_dir.path().join("model.onnx"), b"not-onnx").unwrap();
    assert!(engine
        .hot_swap(invalid_dir.path().to_str().unwrap())
        .is_err());
    assert_eq!(engine.version(), version);
    assert!(engine.is_available());
}
