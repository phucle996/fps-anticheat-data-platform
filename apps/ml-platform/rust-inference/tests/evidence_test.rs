use rust_inference::evidence::EvidenceEngine;

#[test]
fn test_generate_evidence_high_anomaly() {
    // Features: [kills_pm, damage_pm, headshot_ratio, damage_per_kill, movement_pm, perf_vs_lobby]
    let features = [1.50, 750.0, 0.833, 500.0, 250.0, 800.0];
    let matrix = EvidenceEngine::generate_evidence(&features);

    assert!(!matrix.top_evidence_features.is_empty());
    assert!(matrix.top_evidence_features.len() <= 2);

    let first = &matrix.top_evidence_features[0];
    assert!(first.z_score > 2.0);
    assert!(first.reason.contains("Chỉ số"));
}

#[test]
fn test_generate_evidence_normal_player() {
    let features = [0.10, 100.0, 0.15, 80.0, 140.0, 180.0];
    let matrix = EvidenceEngine::generate_evidence(&features);

    assert!(matrix.top_evidence_features.is_empty());
}
