use crate::domain::EventEnvelope;
use tracing::warn;

/// InvalidEnvelopeRecord lưu thông tin sự kiện vi phạm kèm danh sách các lý do vi phạm
#[derive(Debug, Clone)]
pub struct InvalidEnvelopeRecord {
    pub event: EventEnvelope,   // Sự kiện vi phạm
    pub reasons: Vec<String>,    // Danh sách các lý do vi phạm Validation Rules
}

/// ValidationOutcome tổng hợp kết quả phân loại batch
#[derive(Debug, Clone)]
pub struct ValidationOutcome {
    pub valid_records: Vec<EventEnvelope>,            // Danh sách các sự kiện hợp lệ
    pub invalid_records: Vec<InvalidEnvelopeRecord>,  // Danh sách các sự kiện vi phạm
    pub valid_count: usize,                          // Bộ đếm sự kiện hợp lệ
    pub invalid_count: usize,                        // Bộ đếm sự kiện vi phạm
}

/// EventValidator thực thi 11 quy tắc kiểm tra Semantic Data Quality
pub struct EventValidator;

impl EventValidator {
    /// Validate_event kiểm tra 11 quy tắc Semantic Validation trên 1 EventEnvelope
    pub fn validate_event(event: &EventEnvelope) -> Result<(), Vec<String>> {
        let mut reasons = Vec::new();

        // 1. Validate Schema Version
        if event.schema_version.trim().is_empty() {
            reasons.push("schema_version không được để trống".to_string());
        }

        // 2. Validate Event ID (Phải là 64 ký tự SHA-256 Hex)
        let event_id = event.event_id.trim();
        if event_id.is_empty() {
            reasons.push("event_id không được để trống".to_string());
        } else if event_id.len() != 64 || !event_id.chars().all(|c| c.is_ascii_hexdigit()) {
            reasons.push(format!("event_id '{}' không đúng định dạng 64 ký tự SHA-256 Hex", event_id));
        }

        // 3. Validate Operation Code
        if event.op.trim().is_empty() {
            reasons.push("op (operation) không được để trống".to_string());
        }

        // 4. Validate Match ID
        if event.match_id.trim().is_empty() {
            reasons.push("match_id không được để trống".to_string());
        }

        // 5. Validate Player ID
        if event.player_id.trim().is_empty() {
            reasons.push("player_id không được để trống".to_string());
        }

        // 6. Validate Kills >= 0
        if event.payload.kills < 0 {
            reasons.push(format!("kills ({}) không được âm", event.payload.kills));
        }

        // 7. Validate Headshot Kills >= 0
        if event.payload.headshot_kills < 0 {
            reasons.push(format!("headshot_kills ({}) không được âm", event.payload.headshot_kills));
        }

        // 8. Semantic Rule: Headshot Kills <= Kills
        if event.payload.headshot_kills > event.payload.kills {
            reasons.push(format!(
                "headshot_kills ({}) không được lớn hơn tổng số kills ({})",
                event.payload.headshot_kills, event.payload.kills
            ));
        }

        // 9. Validate Damage Dealt >= 0.0
        if event.payload.damage_dealt < 0.0 {
            reasons.push(format!("damage_dealt ({:.2}) không được âm", event.payload.damage_dealt));
        }

        // 10. Validate Distances & Survival Duration >= 0.0
        if event.payload.walk_distance < 0.0 {
            reasons.push(format!("walk_distance ({:.2}) không được âm", event.payload.walk_distance));
        }
        if event.payload.ride_distance < 0.0 {
            reasons.push(format!("ride_distance ({:.2}) không được âm", event.payload.ride_distance));
        }
        if event.payload.swim_distance < 0.0 {
            reasons.push(format!("swim_distance ({:.2}) không được âm", event.payload.swim_distance));
        }
        if event.payload.survival_duration < 0.0 {
            reasons.push(format!("survival_duration ({:.2}) không được âm", event.payload.survival_duration));
        }

        // 11. Validate Win Place Perc (0.0 <= win_place_perc <= 1.0)
        if let Some(win_place) = event.payload.win_place_perc {
            if !(0.0..=1.0).contains(&win_place) {
                reasons.push(format!("win_place_perc ({:.2}) nằm ngoài khoảng hợp lệ [0.0, 1.0]", win_place));
            }
        }

        if reasons.is_empty() {
            Ok(())
        } else {
            Err(reasons)
        }
    }

    /// Validate_batch kiểm tra hàng loạt trong batch và phân loại valid/invalid records
    pub fn validate_batch(events: Vec<EventEnvelope>) -> ValidationOutcome {
        let mut valid_records = Vec::with_capacity(events.len());
        let mut invalid_records = Vec::new();

        for event in events {
            match Self::validate_event(&event) {
                Ok(()) => valid_records.push(event),
                Err(reasons) => {
                    warn!(
                        event_id = %event.event_id,
                        match_id = %event.match_id,
                        player_id = %event.player_id,
                        reasons = ?reasons,
                        "Phát hiện bản ghi EventEnvelope vi phạm Data Quality Validation Rules"
                    );
                    invalid_records.push(InvalidEnvelopeRecord { event, reasons });
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
