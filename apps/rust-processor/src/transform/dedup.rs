use crate::domain::EventEnvelope;
use std::collections::HashSet;
use tracing::info;

/// DeduplicateOutcome chứa danh sách các bản ghi duy nhất và số lượng bản ghi trùng lặp bị loại bỏ
#[derive(Debug, Clone)]
pub struct DeduplicateOutcome {
    pub unique_records: Vec<EventEnvelope>, // Danh sách bản ghi duy nhất (Keep First)
    pub duplicate_count: usize,             // Số lượng bản ghi trùng bị loại bỏ
}

/// EventDeduplicator thực thi lọc trùng lặp bản ghi theo event_id
pub struct EventDeduplicator;

impl EventDeduplicator {
    /// Deduplicate_batch lọc bỏ các event_id xuất hiện trùng lặp trong batch, giữ bản ghi xuất hiện đầu tiên
    pub fn deduplicate_batch(events: Vec<EventEnvelope>) -> DeduplicateOutcome {
        let total_input = events.len();
        let mut seen_ids = HashSet::with_capacity(total_input);
        let mut unique_records = Vec::with_capacity(total_input);

        for event in events {
            if seen_ids.insert(event.event_id.clone()) {
                // Nếu chưa tồn tại trong HashSet -> Thêm vào danh sách duy nhất
                unique_records.push(event);
            }
        }

        let duplicate_count = total_input - unique_records.len();
        if duplicate_count > 0 {
            info!(
                total_input = total_input,
                unique_count = unique_records.len(),
                duplicate_count = duplicate_count,
                "Đã phát hiện và loại bỏ các sự kiện trùng lặp event_id trong Processing Batch"
            );
        }

        DeduplicateOutcome {
            unique_records,
            duplicate_count,
        }
    }
}
