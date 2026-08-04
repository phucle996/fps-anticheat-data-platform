use serde::{Deserialize, Serialize};

/// AnyEnvelope nhận cả EventEnvelope (OpMatchSummary) và KillEventEnvelope (OpKillEventRaw)
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(untagged)]
pub enum AnyEnvelope {
    Kill(KillEventEnvelope),
    PlayerStat(EventEnvelope),
}

impl AnyEnvelope {
    pub fn event_id(&self) -> &str {
        match self {
            AnyEnvelope::Kill(e) => &e.event_id,
            AnyEnvelope::PlayerStat(e) => &e.event_id,
        }
    }

    pub fn match_id(&self) -> &str {
        match self {
            AnyEnvelope::Kill(e) => &e.match_id,
            AnyEnvelope::PlayerStat(e) => &e.match_id,
        }
    }

    pub fn player_id(&self) -> &str {
        match self {
            AnyEnvelope::Kill(e) => &e.player_id,
            AnyEnvelope::PlayerStat(e) => &e.player_id,
        }
    }

    pub fn source(&self) -> &SourceMetadata {
        match self {
            AnyEnvelope::Kill(e) => &e.source,
            AnyEnvelope::PlayerStat(e) => &e.source,
        }
    }
}

/// KillEventEnvelope đóng gói raw kill telemetry event từ Go Ingestor
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct KillEventEnvelope {
    pub schema_version: String,
    pub event_id: String,
    pub op: String,
    pub event_time: Option<String>,
    pub ingest_time: String,
    pub match_id: String,
    pub player_id: String,
    pub source: SourceMetadata,
    pub payload: KillEventPayload,
}

/// KillEventPayload chứa 11 trường telemetry gốc của kill event từ CSV
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct KillEventPayload {
    pub match_id: String,
    pub killer_name: Option<String>,
    pub victim_name: Option<String>,
    pub killer_placement: Option<i32>,
    pub victim_placement: Option<i32>,
    pub killer_position_x: Option<f64>,
    pub killer_position_y: Option<f64>,
    pub victim_position_x: Option<f64>,
    pub victim_position_y: Option<f64>,
    pub event_time_seconds: Option<f64>,
    pub weapon: Option<String>,
}

/// EventEnvelope đóng gói sự kiện Anticheat (OpMatchSummary)
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct EventEnvelope {
    pub schema_version: String,
    pub event_id: String,
    pub op: String,
    pub event_time: Option<String>,
    pub ingest_time: String,
    pub match_id: String,
    pub player_id: String,
    pub source: SourceMetadata,
    pub payload: PlayerStatPayload,
}

/// SourceMetadata lưu thông tin nguồn dataset
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct SourceMetadata {
    pub provider: String,
    pub dataset_id: String,
    pub source_file: String,
    pub record_index: i64,
}

/// PlayerStatPayload chứa các thông số trận đấu của người chơi
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct PlayerStatPayload {
    pub kills: i64,
    pub damage_dealt: f64,
    pub headshot_kills: i64,
    pub walk_distance: f64,
    pub ride_distance: f64,
    pub swim_distance: f64,
    pub survival_duration: f64,
    pub win_place_perc: Option<f64>,
}

/// BatchMetadata lưu thông tin phân đoạn batch
#[derive(Debug, Clone, Serialize, Deserialize)]
#[allow(dead_code)]
pub struct BatchMetadata {
    pub batch_id: String,
    pub record_count: i64,
    pub first_offset: i64,
    pub last_offset: i64,
    pub timestamp: String,
}
