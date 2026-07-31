use crate::error::{AppError, Result};
use serde::{Deserialize, Serialize};
use std::fs;
use std::path::Path;
use tracing::info;

/// RuleConfig định nghĩa thông số quy tắc đánh giá rủi ro trong file YAML
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct RuleConfig {
    pub name: String, // Tên quy tắc (vd: "auto_suspend_aimbot")
    #[serde(default)]
    pub min_score: f32, // Ngưỡng điểm rủi ro tối thiểu (inclusive)
    #[serde(default = "default_max_score")]
    pub max_score: f32, // Ngưỡng điểm rủi ro tối đa (exclusive)
    pub min_headshot_zscore: Option<f32>, // Ngưỡng Z-Score Headshot tối thiểu nếu áp dụng điều kiện phụ
    pub action: String, // Hành động xử lý (CLEAR, WATCHLIST, ESCALATE_TO_MODERATOR, SUSPEND_ACCOUNT, PERMANENT_BAN)
    pub priority: String, // Mức độ ưu tiên xử lý (LOW, MEDIUM, HIGH, URGENT, CRITICAL)
    pub reason: String, // Lý do chi tiết giải thích cho quyết định
}

fn default_max_score() -> f32 {
    1.01
}

/// PolicyConfig chứa cấu hình toàn bộ các rules nạp từ YAML file
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct PolicyConfig {
    pub version: String,        // Phiên bản policy
    pub default_action: String, // Hành động mặc định nếu không match rule nào
    pub rules: Vec<RuleConfig>, // Danh sách các rules xếp theo thứ tự ưu tiên
}

impl PolicyConfig {
    /// Policy là security boundary: thiếu hoặc sai YAML phải fail startup, không
    /// âm thầm đổi hành vi enforcement bằng một fallback trong binary.
    pub fn load_from_file(path: &str) -> Result<Self> {
        if !Path::new(path).is_file() {
            return Err(AppError::Policy(format!(
                "Không tìm thấy policy file tại {path}"
            )));
        }
        let content = fs::read_to_string(path)
            .map_err(|err| AppError::Policy(format!("Không đọc được {path}: {err}")))?;
        let config: PolicyConfig = serde_yaml::from_str(&content)
            .map_err(|err| AppError::Policy(format!("Policy YAML không hợp lệ: {err}")))?;
        config.validate()?;
        info!(
            path,
            rules_count = config.rules.len(),
            version = %config.version,
            "Nạp policy config thành công"
        );
        Ok(config)
    }

    fn validate(&self) -> Result<()> {
        if self.version.trim().is_empty() || self.rules.is_empty() {
            return Err(AppError::Policy(
                "Policy version và rules không được rỗng".to_string(),
            ));
        }
        for rule in &self.rules {
            if rule.name.trim().is_empty()
                || !rule.min_score.is_finite()
                || !rule.max_score.is_finite()
                || rule.min_score < 0.0
                || rule.max_score > 1.01
                || rule.min_score >= rule.max_score
            {
                return Err(AppError::Policy(format!(
                    "Rule '{}' có range không hợp lệ",
                    rule.name
                )));
            }
        }
        Ok(())
    }
}
