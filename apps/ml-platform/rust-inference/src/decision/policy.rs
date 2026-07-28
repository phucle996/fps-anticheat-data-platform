use serde::{Deserialize, Serialize};
use std::fs;
use std::path::Path;
use tracing::{info, warn};

/// RuleConfig định nghĩa thông số quy tắc đánh giá rủi ro trong file YAML
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct RuleConfig {
    pub name: String,                     // Tên quy tắc (vd: "auto_suspend_aimbot")
    #[serde(default)]
    pub min_score: f32,                   // Ngưỡng điểm rủi ro tối thiểu (inclusive)
    #[serde(default = "default_max_score")]
    pub max_score: f32,                   // Ngưỡng điểm rủi ro tối đa (exclusive)
    pub min_headshot_zscore: Option<f32>, // Ngưỡng Z-Score Headshot tối thiểu nếu áp dụng điều kiện phụ
    pub action: String,                   // Hành động xử lý (CLEAR, WATCHLIST, ESCALATE_TO_MODERATOR, SUSPEND_ACCOUNT, PERMANENT_BAN)
    pub priority: String,                 // Mức độ ưu tiên xử lý (LOW, MEDIUM, HIGH, URGENT, CRITICAL)
    pub reason: String,                   // Lý do chi tiết giải thích cho quyết định
}

fn default_max_score() -> f32 {
    1.01
}

/// PolicyConfig chứa cấu hình toàn bộ các rules nạp từ YAML file
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct PolicyConfig {
    pub version: String,            // Phiên bản policy
    pub default_action: String,     // Hành động mặc định nếu không match rule nào
    pub rules: Vec<RuleConfig>,     // Danh sách các rules xếp theo thứ tự ưu tiên
}

impl PolicyConfig {
    /// Load_from_file nạp cấu hình YAML từ đường dẫn chỉ định; tự động tìm kiếm các candidate paths linh hoạt cho cả khi chạy CLI và Cargo Test
    pub fn load_from_file(path: &str) -> Self {
        // Khai báo danh sách các candidate paths để thử nghiệm nạp file (hỗ trợ cả root workspace và crate subfolder khi cargo test)
        let candidate_paths = [
            path.to_string(),
            format!("../../../{}", path),
            format!("../../{}", path),
            "policies.yaml".to_string(),
        ];

        for p in &candidate_paths {
            if Path::new(p).exists() {
                match fs::read_to_string(p) {
                    Ok(content) => match serde_yaml::from_str::<PolicyConfig>(&content) {
                        Ok(config) => {
                            info!(
                                path = %p,
                                rules_count = config.rules.len(),
                                version = %config.version,
                                "Nạp thành công Policy Config từ file YAML!"
                            );
                            return config;
                        }
                        Err(err) => {
                            warn!(
                                error = %err,
                                path = %p,
                                "Lỗi parse định dạng YAML Policy, tự động áp dụng Fallback Policy mặc định"
                            );
                        }
                    },
                    Err(err) => {
                        warn!(
                            error = %err,
                            path = %p,
                            "Không thể đọc file Policy YAML, tự động áp dụng Fallback Policy mặc định"
                        );
                    }
                }
            }
        }

        warn!(
            path = %path,
            "File Policy YAML không tìm thấy ở bất kỳ candidate path nào, khởi tạo Fallback Policy mặc định"
        );

        Self::default_fallback()
    }

    /// Default_fallback tạo cấu hình quy tắc mặc định an toàn cho hệ thống
    pub fn default_fallback() -> Self {
        Self {
            version: "v1-fallback".to_string(),
            default_action: "CLEAR".to_string(),
            rules: vec![
                RuleConfig {
                    name: "fallback_clear".to_string(),
                    min_score: 0.0,
                    max_score: 0.30,
                    min_headshot_zscore: None,
                    action: "CLEAR".to_string(),
                    priority: "LOW".to_string(),
                    reason: "Dung lượng rủi ro trong ngưỡng an toàn mặc định".to_string(),
                },
                RuleConfig {
                    name: "fallback_watchlist".to_string(),
                    min_score: 0.30,
                    max_score: 0.60,
                    min_headshot_zscore: None,
                    action: "WATCHLIST".to_string(),
                    priority: "MEDIUM".to_string(),
                    reason: "Chỉ số bất thường nhẹ, thêm vào danh sách giám sát".to_string(),
                },
                RuleConfig {
                    name: "fallback_review".to_string(),
                    min_score: 0.60,
                    max_score: 0.85,
                    min_headshot_zscore: None,
                    action: "ESCALATE_TO_MODERATOR".to_string(),
                    priority: "HIGH".to_string(),
                    reason: "Rủi ro cao, yêu cầu nhân sự kiểm duyệt".to_string(),
                },
                RuleConfig {
                    name: "fallback_ban".to_string(),
                    min_score: 0.85,
                    max_score: 1.01,
                    min_headshot_zscore: None,
                    action: "PERMANENT_BAN".to_string(),
                    priority: "CRITICAL".to_string(),
                    reason: "Cảnh báo gian lận mức độ nghiêm trọng".to_string(),
                },
            ],
        }
    }
}
