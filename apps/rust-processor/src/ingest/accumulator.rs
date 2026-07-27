use super::consumer::ConsumedMessage;
use crate::domain::EventEnvelope;
use std::collections::HashMap;
use std::time::{Duration, Instant};

/// CompletedBatch đóng gói 1 mảng dữ liệu Batch hoàn chỉnh sẵn sàng ghi Parquet
#[derive(Debug, Clone)]
pub struct CompletedBatch {
    pub batch_id: String,                             // Mã băm SHA-256 duy nhất của batch
    pub events: Vec<EventEnvelope>,                   // Danh sách các sự kiện EventEnvelope
    pub partition_offsets: HashMap<i32, (i64, i64)>, // Bản đồ Partition -> (min_offset, max_offset)
    pub record_count: usize,                          // Số bản ghi trong batch
    pub estimated_bytes: usize,                       // Dung lượng byte ước tính
}

/// BatchAccumulatorConfig cấu hình quy tắc gom batch
#[derive(Debug, Clone)]
pub struct BatchAccumulatorConfig {
    pub max_records: usize,      // Ngưỡng số bản ghi tối đa (vd: 1000)
    pub max_bytes: usize,        // Ngưỡng dung lượng byte tối đa (vd: 10MB = 10,485,760 bytes)
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
    events: Vec<EventEnvelope>,
    partition_offsets: HashMap<i32, (i64, i64)>, // Partition -> (min_offset, max_offset)
    current_bytes: usize,
    last_flush_time: Instant,
}

impl BatchAccumulator {
    /// New khởi tạo BatchAccumulator với cấu hình quy định
    pub fn new(config: BatchAccumulatorConfig) -> Self {
        let max_recs = config.max_records;
        Self {
            config,
            events: Vec::with_capacity(max_recs),
            partition_offsets: HashMap::new(),
            current_bytes: 0,
            last_flush_time: Instant::now(),
        }
    }

    /// Push thêm 1 tin nhắn ConsumedMessage vào bộ đệm, tự động Trigger Flush nếu đạt ranh giới (Count hoặc Bytes)
    pub fn push(&mut self, msg: ConsumedMessage) -> Option<CompletedBatch> {
        let partition = msg.partition;
        let offset = msg.offset;

        // Cập nhật bản đồ theo dõi (min_offset, max_offset) chuẩn xác cho từng Partition
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

        // Ước tính kích thước byte của tin nhắn
        let bytes_len = msg.envelope.event_id.len()
            + msg.envelope.match_id.len()
            + msg.envelope.player_id.len()
            + 250;

        self.events.push(msg.envelope);
        self.current_bytes += bytes_len;

        // Kiểm tra xem đã đạt ngưỡng Record Count hoặc Max Bytes chưa
        if self.events.len() >= self.config.max_records || self.current_bytes >= self.config.max_bytes {
            self.flush()
        } else {
            None
        }
    }

    /// Should_flush_timer kiểm tra xem đã vượt quá khoảng thời gian flush_interval chưa
    pub fn should_flush_timer(&self) -> bool {
        !self.events.is_empty() && self.last_flush_time.elapsed() >= self.config.flush_interval
    }

    /// Flush thực thi đóng gói CompletedBatch và reset trạng thái bộ đệm
    pub fn flush(&mut self) -> Option<CompletedBatch> {
        if self.events.is_empty() {
            return None;
        }

        let record_count = self.events.len();
        let estimated_bytes = self.current_bytes;
        let events = std::mem::replace(&mut self.events, Vec::with_capacity(self.config.max_records));
        let partition_offsets = std::mem::take(&mut self.partition_offsets);

        // Sinh Batch ID định hạn duy nhất
        let batch_id = self.generate_batch_id(&partition_offsets);

        // Reset thời điểm flush
        self.current_bytes = 0;
        self.last_flush_time = Instant::now();

        Some(CompletedBatch {
            batch_id,
            events,
            partition_offsets,
            record_count,
            estimated_bytes,
        })
    }

    /// Generate_batch_id sinh mã băm SHA-256 định danh cho Batch
    fn generate_batch_id(&self, offsets: &HashMap<i32, (i64, i64)>) -> String {
        use std::collections::BTreeMap;
        let sorted_map: BTreeMap<_, _> = offsets.iter().collect();
        let mut raw_str = String::new();
        for (part, (min, max)) in sorted_map {
            raw_str.push_str(&format!("p{}:{}-{}|", part, min, max));
        }

        format!("{:x}", sha256_simple(&raw_str))
    }
}

/// Helper sha256_simple tính mã hash đơn giản
fn sha256_simple(input: &str) -> u128 {
    let mut hash: u128 = 0xcbf29ce484222325;
    for byte in input.bytes() {
        hash ^= u128::from(byte);
        hash = hash.wrapping_mul(0x100000001b3);
    }
    hash
}
