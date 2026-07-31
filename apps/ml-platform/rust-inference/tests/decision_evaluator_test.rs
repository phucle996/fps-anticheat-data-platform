mod common;

use rust_inference::decision::{DecisionEvaluator, PolicyConfig};
use rust_inference::evidence::EvidenceEngine;

// Test_decision_evaluator_policy_rules kiểm tra khả năng đánh giá quyết định rủi ro với các trường hợp khác nhau
#[test]
fn test_decision_evaluator_policy_rules() {
    // 1. Khởi tạo Evaluator với Fallback / Config YAML
    let evaluator = DecisionEvaluator::new(&common::policy_path()).unwrap();

    // Case 1: Risk score thấp (0.15) -> Action: CLEAR
    let features_normal = [10.0, 1.0, 0.1, 100.0, 5.0, 1.0];
    let evidence_normal = EvidenceEngine::generate_evidence(&features_normal);
    let outcome_clear = evaluator.evaluate(0.15, &evidence_normal, &features_normal);
    assert_eq!(outcome_clear.action, "CLEAR");
    assert_eq!(outcome_clear.priority, "LOW");

    // Case 2: Risk score trung bình (0.45) -> Action: WATCHLIST
    let outcome_watchlist = evaluator.evaluate(0.45, &evidence_normal, &features_normal);
    assert_eq!(outcome_watchlist.action, "WATCHLIST");

    // Case 3: Risk score cao (0.75) -> Action: ESCALATE_TO_MODERATOR
    let outcome_review = evaluator.evaluate(0.75, &evidence_normal, &features_normal);
    assert_eq!(outcome_review.action, "ESCALATE_TO_MODERATOR");

    // Case 4: Risk score cực cao (0.90) + Headshot ratio rất cao -> Action: SUSPEND_ACCOUNT
    let features_aimbot = [0.15, 120.0, 0.95, 100.0, 150.0, 200.0]; // Headshot ratio = 0.95 (Z-score ~ 6.5)
    let evidence_aimbot = EvidenceEngine::generate_evidence(&features_aimbot);
    let outcome_suspend = evaluator.evaluate(0.90, &evidence_aimbot, &features_aimbot);
    assert_eq!(outcome_suspend.action, "SUSPEND_ACCOUNT");
    assert_eq!(outcome_suspend.priority, "URGENT");

    // Case 5: Heuristic Teleport Hack (spatial_teleport_score > 0.80) -> Instant PERMANENT_BAN
    let features_teleport = vec![
        8.0, 500.0, 0.5, 100.0, 10.0, 500.0, 100.0, 0.0, 5000.0, 0.95, 2.0,
    ];
    let evidence_teleport = EvidenceEngine::generate_evidence(&features_teleport);
    let outcome_teleport = evaluator.evaluate(0.99, &evidence_teleport, &features_teleport);
    assert_eq!(outcome_teleport.action, "PERMANENT_BAN");
    assert_eq!(outcome_teleport.policy_rule, "heuristic_teleport_hack");
}

// Policy là security boundary nên file thiếu phải fail-close.
#[test]
fn test_policy_config_fallback_safety() {
    assert!(PolicyConfig::load_from_file("non_existent_policy.yaml").is_err());
}
