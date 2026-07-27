use serde::{Deserialize, Serialize};
use std::collections::HashMap;

/// PartitionOffsetMetadata lưu vị trí offset min/max của một Kafka Partition
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct PartitionOffsetMetadata {
    pub min_offset: i64, // Offset bắt đầu trong batch
    pub max_offset: i64, // Offset kết thúc trong batch
}

/// BatchManifest lưu vết audit log cho từng processing batch được ghi xuống MinIO S3
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct BatchManifest {
    pub batch_id: String,                                          // Mã băm định danh batch duy nhất
    pub source_topic: String,                                      // Topic Kafka nguồn (pubg.v1.player-stat.raw)
    pub partition_offsets: HashMap<i32, PartitionOffsetMetadata>, // Bản đồ Partition -> (min_offset, max_offset)
    pub total_records_read: usize,                                 // Tổng số bản ghi tiêu thụ từ Kafka
    pub valid_records_count: usize,                               // Số bản ghi hợp lệ
    pub invalid_records_count: usize,                             // Số bản ghi vi phạm Data Quality
    pub duplicate_records_count: usize,                           // Số bản ghi trùng lặp bị loại bỏ
    pub data_object_path: String,                                  // Đường dẫn S3 Parquet Bronze file
    pub checksum_sha256: String,                                   // Mã băm SHA-256 xác thực Parquet file
    pub processing_timestamp: String,                              // Thời điểm hoàn tất xử lý (RFC3339 UTC)
}
