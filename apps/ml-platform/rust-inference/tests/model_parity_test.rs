mod common;

use rust_inference::inference::OnnxInferenceEngine;

#[test]
fn test_model_parity_with_python_onnxruntime_reference() {
    let temp_dir = tempfile::tempdir().unwrap();
    common::write_test_bundle(temp_dir.path());
    let engine = OnnxInferenceEngine::new(temp_dir.path().to_str().unwrap()).unwrap();

    let cases = [
        ([1.0, 10.0, 20.0, 2.0, 3.0], 0.6681878_f32),
        ([8.0, 1.0, 1.0, 5.0, 4.0], 0.99310476_f32),
    ];
    for (features, python_onnxruntime_score) in cases {
        let (rust_score, _) = engine.predict(&features).unwrap();
        assert!(
            (rust_score - python_onnxruntime_score).abs() <= 1e-5,
            "Rust={rust_score}, Python={python_onnxruntime_score}"
        );
    }
}
