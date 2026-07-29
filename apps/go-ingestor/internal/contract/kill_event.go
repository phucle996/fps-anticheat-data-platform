package contract

import "time"

// OpKillEventRaw — discriminator cho raw kill telemetry events từ match_deaths schema
const OpKillEventRaw = "data.kill_event.raw"

// KillEventPayload đại diện cho payload kill telemetry thô giữ nguyên 11 trường từ Kaggle CSV
// Sử dụng kiểu con trỏ (*float64, *int, *string) để hỗ trợ nullable — trường nguồn thiếu sẽ là nil/null,
// tuyệt đối KHÔNG tự ép về 0 hay aliasing sang field khác (tránh sai lệch ngữ nghĩa).
type KillEventPayload struct {
	MatchID          string   `json:"match_id"`
	KillerName       *string  `json:"killer_name"`
	VictimName       *string  `json:"victim_name"`
	KillerPlacement  *int     `json:"killer_placement"`
	VictimPlacement  *int     `json:"victim_placement"`
	KillerPositionX  *float64 `json:"killer_position_x"`
	KillerPositionY  *float64 `json:"killer_position_y"`
	VictimPositionX  *float64 `json:"victim_position_x"`
	VictimPositionY  *float64 `json:"victim_position_y"`
	EventTimeSeconds *float64 `json:"event_time_seconds"`
	Weapon           *string  `json:"weapon"`
}

// KillEventEnvelope bao bọc KillEventPayload kèm Event Metadata và Source Lineage
type KillEventEnvelope struct {
	SchemaVersion string           `json:"schema_version"` // Phiên bản schema ("1.0")
	EventID       string           `json:"event_id"`       // Mã SHA-256 duy nhất dùng cho dedup
	Op            string           `json:"op"`             // Hằng số OpKillEventRaw
	EventTime     *time.Time       `json:"event_time"`     // Thời gian sự kiện (nếu có)
	IngestTime    time.Time        `json:"ingest_time"`    // Thời gian Ingestor nạp (ISO 8601 UTC)
	MatchID       string           `json:"match_id"`       // Mã trận đấu (Kafka Message Key)
	PlayerID      string           `json:"player_id"`      // Killer name hoặc Victim name (nếu killer nil)
	Source        SourceMetadata   `json:"source"`         // Source lineage metadata
	Payload       KillEventPayload `json:"payload"`        // Raw telemetry payload
}
