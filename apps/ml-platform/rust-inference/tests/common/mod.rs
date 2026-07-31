use base64::Engine;
use sha2::{Digest, Sha256};
use std::fs;
use std::path::Path;

const TEST_MODEL_BASE64: &str = "CAoSCHNrbDJvbm54GgYxLjIwLjAiB2FpLm9ubngoADIAOvUCCoECCgtmbG9hdF9pbnB1dBIFbGFiZWwSDXByb2JhYmlsaXRpZXMaEExpbmVhckNsYXNzaWZpZXIiEExpbmVhckNsYXNzaWZpZXIqGQoQY2xhc3NsYWJlbHNfaW50c0AAQAGgAQcqQwoMY29lZmZpY2llbnRzPdNedL09YxidPT2i0iU+Pdp10bw95qMLvD3TXnQ9PWMYnb09otIlvj3addE8PeajCzygAQYqGQoKaW50ZXJjZXB0cz28OJLAPbw4kkCgAQYqEgoLbXVsdGlfY2xhc3MYAKABAiodCg5wb3N0X3RyYW5zZm9ybSIITE9HSVNUSUOgAQM6CmFpLm9ubngubWwSIDgzMjQyNDc2NzVkOTQzYWY5YjE5NjZjNmY3NDg1MjdkWhsKC2Zsb2F0X2lucHV0EgwKCggBEgYKAAoCCAViEQoFbGFiZWwSCAoGCAcSAgoAYh0KDXByb2JhYmlsaXRpZXMSDAoKCAESBgoACgIIAkIOCgphaS5vbm54Lm1sEAFCBAoAEBZCBAoAEBY=";

pub fn write_test_bundle(directory: &Path) {
    fs::create_dir_all(directory).unwrap();
    let model = base64::engine::general_purpose::STANDARD
        .decode(TEST_MODEL_BASE64)
        .unwrap();
    let checksum = format!("{:x}", Sha256::digest(&model));
    fs::write(directory.join("model.onnx"), model).unwrap();
    fs::write(
        directory.join("feature_schema.json"),
        r#"{
          "model_name":"pubg-risk",
          "model_version":"test-v1",
          "input_dtype":"float32",
          "input_shape":["batch_size",5],
          "features":["kills","minimum_kill_interval_seconds","median_kill_distance_coordinate_units","short_kill_interval_count","unique_weapons_used"]
        }"#,
    )
    .unwrap();
    fs::write(
        directory.join("threshold_policy.json"),
        r#"{"model_version":"test-v1","thresholds":{"LOW":0.0,"MEDIUM":0.3,"HIGH":0.6,"CRITICAL":0.9}}"#,
    )
    .unwrap();
    fs::write(
        directory.join("checksums.sha256"),
        format!(r#"{{"model.onnx":"{checksum}"}}"#),
    )
    .unwrap();
}

pub fn policy_path() -> String {
    Path::new(env!("CARGO_MANIFEST_DIR"))
        .join("../../../configs/policies.yaml")
        .to_string_lossy()
        .into_owned()
}
