use crate::domain::{AnyEnvelope, EventEnvelope, KillEventEnvelope};
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
            reasons.push(format!(
                "event_id '{}' không đúng định dạng 64 ký tự SHA-256 Hex",
                event_id
            ));
        }

        Self::validate_identifier("match_id", event.match_id(), &mut reasons);
        Self::validate_identifier("player_id", event.player_id(), &mut reasons);

        match event {
            AnyEnvelope::PlayerStat(envelope) => Self::validate_player_stat(envelope, &mut reasons),
            AnyEnvelope::Kill(envelope) => Self::validate_kill_event(envelope, &mut reasons),
        }

        if reasons.is_empty() {
            Ok(())
        } else {
            Err(reasons)
        }
    }

    fn validate_identifier(field: &str, value: &str, reasons: &mut Vec<String>) {
        let value = value.trim();
        if value.is_empty() {
            reasons.push(format!("{} không được để trống", field));
        } else if value.len() > 256 {
            reasons.push(format!("{} vượt quá giới hạn 256 byte", field));
        }
    }

    fn validate_common_times(
        schema_version: &str,
        ingest_time: &str,
        event_time: Option<&str>,
        reasons: &mut Vec<String>,
    ) {
        if schema_version != "1.0" {
            reasons.push(format!(
                "schema_version '{}' không được hỗ trợ",
                schema_version
            ));
        }
        if chrono::DateTime::parse_from_rfc3339(ingest_time).is_err() {
            reasons.push("ingest_time phải là RFC3339 hợp lệ".to_string());
        }
        if event_time.is_some_and(|value| chrono::DateTime::parse_from_rfc3339(value).is_err()) {
            reasons.push("event_time phải là RFC3339 hợp lệ".to_string());
        }
    }

    fn validate_player_stat(event: &EventEnvelope, reasons: &mut Vec<String>) {
        Self::validate_common_times(
            &event.schema_version,
            &event.ingest_time,
            event.event_time.as_deref(),
            reasons,
        );
        if !matches!(
            event.op.as_str(),
            "data.player_stat.match_summary" | "data.kill_event.kill_death"
        ) {
            reasons.push(format!(
                "op '{}' không hợp lệ cho player-stat envelope",
                event.op
            ));
        }

        let payload = &event.payload;
        if payload.kills < 0 {
            reasons.push("kills không được âm".to_string());
        }
        if payload.headshot_kills < 0 {
            reasons.push("headshot_kills không được âm".to_string());
        }
        if payload.headshot_kills > payload.kills {
            reasons.push("headshot_kills không được lớn hơn kills".to_string());
        }
        for (field, value) in [
            ("damage_dealt", payload.damage_dealt),
            ("walk_distance", payload.walk_distance),
            ("ride_distance", payload.ride_distance),
            ("swim_distance", payload.swim_distance),
            ("survival_duration", payload.survival_duration),
        ] {
            if !value.is_finite() || value < 0.0 {
                reasons.push(format!("{} phải là số hữu hạn không âm", field));
            }
        }
        if payload
            .win_place_perc
            .is_some_and(|value| !value.is_finite() || !(0.0..=1.0).contains(&value))
        {
            reasons.push("win_place_perc phải nằm trong [0.0, 1.0]".to_string());
        }
    }

    fn validate_kill_event(event: &KillEventEnvelope, reasons: &mut Vec<String>) {
        Self::validate_common_times(
            &event.schema_version,
            &event.ingest_time,
            event.event_time.as_deref(),
            reasons,
        );
        if event.op != "data.kill_event.raw" {
            reasons.push(format!(
                "op '{}' không hợp lệ cho kill-event envelope",
                event.op
            ));
        }
        if event.payload.match_id != event.match_id {
            // Hai match_id không đồng nhất sẽ phá ordering key và partition-local replay.
            reasons.push("payload.match_id phải trùng envelope.match_id".to_string());
        }
        for (field, value) in [
            ("killer_placement", event.payload.killer_placement),
            ("victim_placement", event.payload.victim_placement),
        ] {
            if value.is_some_and(|placement| placement <= 0) {
                reasons.push(format!("{} phải lớn hơn 0 khi có giá trị", field));
            }
        }
        for (field, value) in [
            ("killer_position_x", event.payload.killer_position_x),
            ("killer_position_y", event.payload.killer_position_y),
            ("victim_position_x", event.payload.victim_position_x),
            ("victim_position_y", event.payload.victim_position_y),
            ("event_time_seconds", event.payload.event_time_seconds),
        ] {
            if value.is_some_and(|metric| !metric.is_finite()) {
                reasons.push(format!("{} phải là số hữu hạn", field));
            }
        }
        if event
            .payload
            .event_time_seconds
            .is_some_and(|seconds| seconds < 0.0)
        {
            reasons.push("event_time_seconds không được âm".to_string());
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
