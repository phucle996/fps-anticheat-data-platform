use crate::decision::policy::PolicyConfig;
use crate::error::Result;
use crate::evidence::EvidenceMatrix;
use serde::{Deserialize, Serialize};
use std::sync::{Arc, RwLock};

/// DecisionOutcome định nghĩa cấu trúc kết quả quyết định xử lý gian lận
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct DecisionOutcome {
    pub action: String, // Hành động xử lý: "CLEAR", "WATCHLIST", "ESCALATE_TO_MODERATOR", "SUSPEND_ACCOUNT", "PERMANENT_BAN"
    pub priority: String, // Mức độ ưu tiên: "LOW", "MEDIUM", "HIGH", "URGENT", "CRITICAL"
    pub reason: String, // Lý do giải thích cho quyết định xử lý
    pub policy_rule: String, // Tên quy tắc (rule) khớp điều kiện
    pub policy_version: String, // Phiên bản policy YAML áp dụng
}

/// DecisionEvaluator thực thi đánh giá kết hợp ML Risk Score và Evidence Matrix với quy tắc Policy YAML
#[derive(Clone)]
pub struct DecisionEvaluator {
    config: Arc<RwLock<PolicyConfig>>, // Thread-safe atomic pointer hỗ trợ đọc đồng thời không gây nghẽn luồng
}

impl DecisionEvaluator {
    /// New khởi tạo DecisionEvaluator từ đường dẫn file cấu hình Policy YAML
    pub fn new(policy_path: &str) -> Result<Self> {
        let config = PolicyConfig::load_from_file(policy_path)?;
        Ok(Self {
            config: Arc::new(RwLock::new(config)),
        })
    }

    /// Evaluate thực hiện đối chiếu điểm rủi ro và các chỉ số bằng chứng để đưa ra kết quả xử lý
    pub fn evaluate(
        &self,
        risk_score: f32,
        evidence: &EvidenceMatrix,
        raw_features: &[f32],
    ) -> DecisionOutcome {
        // Tầng 1: Đánh giá quy tắc Heuristic Siêu Bằng Chứng (Instant Physics Bound Violation Rules)
        if raw_features.len() >= 10 {
            let kills = raw_features.get(0).cloned().unwrap_or(0.0);
            let burst_interval = raw_features.get(8).cloned().unwrap_or(0.0);
            let teleport_score = raw_features.get(9).cloned().unwrap_or(0.0);
            let headshot_streak = raw_features.get(10).cloned().unwrap_or(0.0);

            // Rule 1: Teleport Hack (Dịch chuyển dị thường trên bản đồ)
            if teleport_score > 0.80 {
                return DecisionOutcome {
                    action: "PERMANENT_BAN".to_string(),
                    priority: "CRITICAL".to_string(),
                    reason: format!(
                        "Phát hiện Teleport Hack: Chỉ số dịch chuyển vị trí dị thường {:.2}",
                        teleport_score
                    ),
                    policy_rule: "heuristic_teleport_hack".to_string(),
                    policy_version: "v1.0-heuristic".to_string(),
                };
            }

            // Rule 2: Burst Aimbot (Hạ gục liên tiếp dưới 200ms)
            if kills >= 2.0 && burst_interval > 0.0 && burst_interval < 200.0 {
                return DecisionOutcome {
                    action: "PERMANENT_BAN".to_string(),
                    priority: "CRITICAL".to_string(),
                    reason: format!(
                        "Phát hiện Burst Aimbot: Thời gian hạ gục liên tiếp {:.1}ms siêu tốc",
                        burst_interval
                    ),
                    policy_rule: "heuristic_burst_aimbot".to_string(),
                    policy_version: "v1.0-heuristic".to_string(),
                };
            }

            // Rule 3: Headshot Streak (Chuỗi 5 pha Headshot liên tiếp)
            if headshot_streak >= 5.0 {
                return DecisionOutcome {
                    action: "PERMANENT_BAN".to_string(),
                    priority: "CRITICAL".to_string(),
                    reason: format!(
                        "Phát hiện Headshot Lock: Chuỗi {:.0} lần hạ gục Headshot liên tiếp",
                        headshot_streak
                    ),
                    policy_rule: "heuristic_headshot_streak".to_string(),
                    policy_version: "v1.0-heuristic".to_string(),
                };
            }
        }

        // Tầng 2: Đánh giá Policy Config từ file YAML theo ML Risk Score
        let config_guard = match self.config.read() {
            Ok(guard) => guard,
            Err(_) => {
                // Lock poison không được phép tạo quyết định CLEAR fail-open.
                return DecisionOutcome {
                    action: "ESCALATE_TO_MODERATOR".to_string(),
                    priority: "HIGH".to_string(),
                    reason: "Policy state không khả dụng; yêu cầu đánh giá thủ công".to_string(),
                    policy_rule: "fallback_lock_error".to_string(),
                    policy_version: "UNAVAILABLE".to_string(),
                };
            }
        };

        // Trích xuất chỉ số Headshot Z-Score từ tập bằng chứng top_evidence_features trong EvidenceMatrix
        let headshot_zscore = evidence
            .top_evidence_features
            .iter()
            .find(|item| item.feature == "headshot_ratio")
            .map(|item| item.z_score)
            .unwrap_or(0.0);

        // Lặp qua từng rule trong cấu hình YAML theo thứ tự ưu tiên
        for rule in &config_guard.rules {
            // Kiểm tra điều kiện 1: risk_score thuộc khoảng [min_score, max_score)
            if risk_score >= rule.min_score && risk_score < rule.max_score {
                // Kiểm tra điều kiện 2: Chỉ số Headshot Z-Score có thỏa mãn yêu cầu tối thiểu (nếu rule có định nghĩa)
                if let Some(min_z) = rule.min_headshot_zscore {
                    if headshot_zscore < min_z {
                        // Không đạt chỉ số Headshot Z-Score -> Bỏ qua rule này để chuyển sang kiểm tra rule tiếp theo
                        continue;
                    }
                }

                // Đã khớp hoàn toàn cả điều kiện điểm số và chỉ số bằng chứng!
                return DecisionOutcome {
                    action: rule.action.clone(),
                    priority: rule.priority.clone(),
                    reason: rule.reason.clone(),
                    policy_rule: rule.name.clone(),
                    policy_version: config_guard.version.clone(),
                };
            }
        }

        // Trường hợp không có rule nào phù hợp, áp dụng hành động mặc định
        DecisionOutcome {
            action: config_guard.default_action.clone(),
            priority: "LOW".to_string(),
            reason: "Không khớp quy tắc cụ thể, áp dụng hành động mặc định".to_string(),
            policy_rule: "default_action".to_string(),
            policy_version: config_guard.version.clone(),
        }
    }
}
