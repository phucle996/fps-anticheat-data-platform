use serde::{Deserialize, Serialize};

/// EventEnvelope đóng gói sự kiện Anticheat được chuẩn hóa từ Go Ingestor
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct EventEnvelope {
    pub schema_version: String,        // Phiên bản schema (vd: "1.0")
    pub event_id: String,              // SHA-256 Event ID định hạn
    pub op: String,                    // Mã thao tác (vd: "data.player_stat.match_summary")
    pub event_time: Option<String>,    // Thời điểm sự kiện (nếu có)
    pub ingest_time: String,           // Thời điểm Go Ingestor nhận dữ liệu (RFC3339 UTC)
    pub match_id: String,              // ID trận đấu PUBG
    pub player_id: String,             // ID người chơi PUBG
    pub source: SourceMetadata,        // Metadata nguồn Kaggle dataset
    pub payload: PlayerStatPayload,    // Chỉ số thống kê trận đấu
}

/// SourceMetadata lưu thông tin nguồn dataset
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct SourceMetadata {
    pub provider: String,     // Nhà cung cấp dữ liệu (vd: "kaggle")
    pub dataset_id: String,   // Slug/ID của Kaggle dataset
    pub source_file: String,  // Tên file CSV nguồn (vd: train_V2.csv)
    pub record_index: i64,    // Index dòng trong file CSV
}

/// PlayerStatPayload chứa các thông số trận đấu của người chơi
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct PlayerStatPayload {
    pub kills: i64,                    // Số mạng tiêu diệt
    pub damage_dealt: f64,             // Tổng sát thương gây ra
    pub headshot_kills: i64,           // Số mạng tiêu diệt bằng headshot
    pub walk_distance: f64,            // Khoảng cách di chuyển bộ (m)
    pub ride_distance: f64,            // Khoảng cách di chuyển xe (m)
    pub swim_distance: f64,            // Khoảng cách bơi (m)
    pub survival_duration: f64,        // Thời gian sống sót (s)
    pub win_place_perc: Option<f64>,   // Tỷ lệ xếp hạng thắng (0.0 - 1.0)
}

/// BatchMetadata lưu thông tin phân đoạn batch
#[derive(Debug, Clone, Serialize, Deserialize)]
#[allow(dead_code)]
pub struct BatchMetadata {
    pub batch_id: String,               // Mã băm batch
    pub record_count: i64,              // Tổng số bản ghi trong batch
    pub first_offset: i64,              // Offset đầu tiên trong batch
    pub last_offset: i64,               // Offset cuối cùng trong batch
    pub timestamp: String,              // Thời điểm đóng gói batch
}
