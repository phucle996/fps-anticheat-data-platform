use crate::domain::AnyEnvelope;
use serde::{Deserialize, Serialize};
use tracing::warn;

/// InvalidEnvelopeRecord lưu thông tin sự kiện vi phạm
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct InvalidEnvelopeRecord {
    pub event_id: String,
    pub match_id: String,
    pub player_id: String,
    pub reasons: Vec<String>,
}

/// ValidationOutcome tổng hợp kết quả phân loại batch
#[derive(Debug, Clone)]
pub struct ValidationOutcome {
    pub valid_records: Vec<AnyEnvelope>,
    pub invalid_records: Vec<InvalidEnvelopeRecord>,
    pub valid_count: usize,
    pub invalid_count: usize,
}

/// EventValidator thực thi quy tắc kiểm tra Semantic Data Quality
pub struct EventValidator;

impl EventValidator {
    pub fn validate_any(event: &AnyEnvelope) -> Result<(), Vec<String>> {
        let mut reasons = Vec::new();

        let event_id = event.event_id().trim();
        if event_id.is_empty() {
            reasons.push("event_id không được để trống".to_string());
        } else if event_id.len() != 64 || !event_id.chars().all(|c| c.is_ascii_hexdigit()) {
            reasons.push(format!("event_id '{}' không đúng định dạng 64 ký tự SHA-256 Hex", event_id));
        }

        if event.match_id().trim().is_empty() {
            reasons.push("match_id không được để trống".to_string());
        }

        if event.player_id().trim().is_empty() {
            reasons.push("player_id không được để trống".to_string());
        }

        if reasons.is_empty() {
            Ok(())
        } else {
            Err(reasons)
        }
    }

    pub fn validate_batch(events: Vec<AnyEnvelope>) -> ValidationOutcome {
        let mut valid_records = Vec::with_capacity(events.len());
        let mut invalid_records = Vec::new();

        for event in events {
            match Self::validate_any(&event) {
                Ok(()) => valid_records.push(event),
                Err(reasons) => {
                    warn!(
                        event_id = %event.event_id(),
                        match_id = %event.match_id(),
                        player_id = %event.player_id(),
                        reasons = ?reasons,
                        "Phát hiện bản ghi AnyEnvelope vi phạm Data Quality Validation Rules"
                    );
                    invalid_records.push(InvalidEnvelopeRecord {
                        event_id: event.event_id().to_string(),
                        match_id: event.match_id().to_string(),
                        player_id: event.player_id().to_string(),
                        reasons,
                    });
                }
            }
        }

        let valid_count = valid_records.len();
        let invalid_count = invalid_records.len();

        ValidationOutcome {
            valid_records,
            invalid_records,
            valid_count,
            invalid_count,
        }
    }
}
