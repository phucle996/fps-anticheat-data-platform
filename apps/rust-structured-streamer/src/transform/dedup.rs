use crate::domain::AnyEnvelope;
use std::collections::HashSet;

/// DeduplicateOutcome tổng hợp kết quả lọc trùng lặp
#[derive(Debug, Clone)]
pub struct DeduplicateOutcome {
    pub unique_records: Vec<AnyEnvelope>, // Danh sách các bản ghi độc nhất
    pub duplicate_count: usize,           // Số lượng bản ghi bị loại bỏ do trùng event_id
}

/// EventDeduplicator thực thi khử trùng lặp dữ liệu (Deduplication)
pub struct EventDeduplicator;

impl EventDeduplicator {
    /// Deduplicate_batch lọc trùng lặp mảng AnyEnvelope theo SHA-256 event_id
    pub fn deduplicate_batch(events: Vec<AnyEnvelope>) -> DeduplicateOutcome {
        let mut seen_event_ids = HashSet::with_capacity(events.len());
        let mut unique_records = Vec::with_capacity(events.len());
        let mut duplicate_count = 0usize;

        for event in events {
            let event_id = event.event_id().to_string();

            if seen_event_ids.contains(&event_id) {
                duplicate_count += 1;
            } else {
                seen_event_ids.insert(event_id);
                unique_records.push(event);
            }
        }

        DeduplicateOutcome {
            unique_records,
            duplicate_count,
        }
    }
}
