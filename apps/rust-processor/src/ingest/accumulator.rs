use super::consumer::{ConsumedMessage, InvalidKafkaMessage};
use crate::domain::AnyEnvelope;
use sha2::{Digest, Sha256};
use std::collections::HashMap;
use std::time::{Duration, Instant};

/// CompletedBatch đóng gói 1 mảng dữ liệu Batch hoàn chỉnh sẵn sàng ghi Parquet
#[derive(Debug, Clone)]
pub struct CompletedBatch {
    pub batch_id: String,         // Mã băm SHA-256 duy nhất của batch
    pub events: Vec<AnyEnvelope>, // Danh sách các sự kiện AnyEnvelope
    pub malformed_messages: Vec<InvalidKafkaMessage>, // Payload không parse được, phải durable trước khi commit
    pub partition_offsets: HashMap<i32, (i64, i64)>, // Bản đồ Partition -> (min_offset, max_offset)
    pub record_count: usize,                         // Số bản ghi trong batch
    pub estimated_bytes: usize,                      // Dung lượng byte ước tính
}

/// BatchAccumulatorConfig cấu hình quy tắc gom batch
#[derive(Debug, Clone)]
pub struct BatchAccumulatorConfig {
    pub max_records: usize,       // Ngưỡng số bản ghi tối đa (vd: 1000)
    pub max_bytes: usize,         // Ngưỡng dung lượng byte tối đa (vd: 10MB)
    pub flush_interval: Duration, // Nhịp timer flush tối đa (vd: 1000ms)
}

impl Default for BatchAccumulatorConfig {
    fn default() -> Self {
        Self {
            max_records: 1000,
            max_bytes: 10 * 1024 * 1024,
            flush_interval: Duration::from_millis(1000),
        }
    }
}

/// BatchAccumulator gom tụ các sự kiện Kafka trong bộ nhớ RAM trước khi đẩy sang Apache Arrow & Parquet
pub struct BatchAccumulator {
    config: BatchAccumulatorConfig,
    events: Vec<AnyEnvelope>,
    malformed_messages: Vec<InvalidKafkaMessage>,
    partition_offsets: HashMap<i32, (i64, i64)>,
    current_bytes: usize,
    last_flush_time: Instant,
}

impl BatchAccumulator {
    /// New khởi tạo BatchAccumulator
    pub fn new(config: BatchAccumulatorConfig) -> Self {
        let max_recs = config.max_records;
        Self {
            config,
            events: Vec::with_capacity(max_recs),
            malformed_messages: Vec::new(),
            partition_offsets: HashMap::new(),
            current_bytes: 0,
            last_flush_time: Instant::now(),
        }
    }

    /// Push thêm 1 tin nhắn ConsumedMessage vào bộ đệm
    pub fn push(&mut self, msg: ConsumedMessage) -> Option<CompletedBatch> {
        let partition = msg.partition;
        let offset = msg.offset;

        self.track_partition_offset(partition, offset);

        let bytes_len = msg.envelope.event_id().len()
            + msg.envelope.match_id().len()
            + msg.envelope.player_id().len()
            + 250;

        self.events.push(msg.envelope);
        self.current_bytes += bytes_len;

        if self.pending_count() >= self.config.max_records
            || self.current_bytes >= self.config.max_bytes
        {
            self.flush()
        } else {
            None
        }
    }

    /// Push_invalid giữ nguyên malformed payload trong batch cho tới khi DLQ đã durable.
    pub fn push_invalid(&mut self, message: InvalidKafkaMessage) -> Option<CompletedBatch> {
        self.track_partition_offset(message.partition, message.offset);
        self.current_bytes += message.raw_payload.len() + message.error_reason.len() + 128;
        self.malformed_messages.push(message);

        if self.pending_count() >= self.config.max_records
            || self.current_bytes >= self.config.max_bytes
        {
            self.flush()
        } else {
            None
        }
    }

    fn track_partition_offset(&mut self, partition: i32, offset: i64) {
        self.partition_offsets
            .entry(partition)
            .and_modify(|(min, max)| {
                if offset < *min {
                    *min = offset;
                }
                if offset > *max {
                    *max = offset;
                }
            })
            .or_insert((offset, offset));
    }

    /// Should_flush_timer kiểm tra xem đã vượt quá khoảng thời gian flush_interval chưa
    pub fn should_flush_timer(&self) -> bool {
        self.pending_count() > 0 && self.last_flush_time.elapsed() >= self.config.flush_interval
    }

    /// Flush thực thi đóng gói CompletedBatch và reset trạng thái bộ đệm
    pub fn flush(&mut self) -> Option<CompletedBatch> {
        if self.events.is_empty() && self.partition_offsets.is_empty() {
            return None;
        }

        let record_count = self.pending_count();
        let estimated_bytes = self.current_bytes;
        let events = std::mem::replace(
            &mut self.events,
            Vec::with_capacity(self.config.max_records),
        );
        let malformed_messages = std::mem::take(&mut self.malformed_messages);
        let partition_offsets = std::mem::take(&mut self.partition_offsets);

        let batch_id = self.generate_batch_id(&partition_offsets);

        self.current_bytes = 0;
        self.last_flush_time = Instant::now();

        Some(CompletedBatch {
            batch_id,
            events,
            malformed_messages,
            partition_offsets,
            record_count,
            estimated_bytes,
        })
    }

    fn generate_batch_id(&self, offsets: &HashMap<i32, (i64, i64)>) -> String {
        use std::collections::BTreeMap;
        let sorted_map: BTreeMap<_, _> = offsets.iter().collect();
        let mut raw_str = String::new();
        for (part, (min, max)) in sorted_map {
            raw_str.push_str(&format!("p{}:{}-{}|", part, min, max));
        }

        format!("{:x}", Sha256::digest(raw_str.as_bytes()))
    }

    fn pending_count(&self) -> usize {
        self.events.len() + self.malformed_messages.len()
    }
}
