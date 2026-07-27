package contract

import (
	"time"
)

// EventEnvelope đại diện cho cấu trúc tin nhắn sự kiện hợp lệ gửi vào Kafka Topic (pubg.v1.player-stat.raw)
type EventEnvelope struct {
	SchemaVersion string            `json:"schema_version"` // Phiên bản schema dữ liệu (mặc định "1.0")
	EventID       string            `json:"event_id"`       // Chuỗi băm SHA-256 duy nhất dùng cho khử trùng lặp
	Op            string            `json:"op"`             // Tên thao tác (data.player_stat.match_summary)
	EventTime     *time.Time        `json:"event_time"`     // Thời gian xảy ra sự kiện trong game (nếu có)
	IngestTime    time.Time         `json:"ingest_time"`    // Thời gian Go Ingestor nạp bản ghi (ISO 8601 UTC)
	MatchID       string            `json:"match_id"`       // Mã trận đấu (Message Key của Kafka)
	PlayerID      string            `json:"player_id"`      // Mã người chơi
	Source        SourceMetadata    `json:"source"`         // Metadata truy xuất nguồn gốc dữ liệu
	Payload       PlayerStatPayload `json:"payload"`        // Payload chứa các chỉ số thống kê của player
}

// SourceMetadata định nghĩa thông tin vị trí xuất xứ dữ liệu nguồn
type SourceMetadata struct {
	Provider    string `json:"provider"`     // Tên nhà cung cấp (kaggle)
	DatasetID   string `json:"dataset_id"`   // ID dataset
	SourceFile  string `json:"source_file"`  // Tên file CSV nguồn (train_V2.csv)
	RecordIndex int64  `json:"record_index"` // Chỉ số dòng bản ghi trong file CSV
}

// PlayerStatPayload định nghĩa các chỉ số thống kê trận đấu của người chơi
type PlayerStatPayload struct {
	Kills            int64    `json:"kills"`              // Số mạng hạ gục
	DamageDealt      float64  `json:"damage_dealt"`       // Lượng sát thương gây ra
	HeadshotKills    int64    `json:"headshot_kills"`     // Số mạng hạ gục bằng headshot
	WalkDistance     float64  `json:"walk_distance"`      // Khoảng cách đi bộ (mét)
	RideDistance     float64  `json:"ride_distance"`      // Khoảng cách đi xe (mét)
	SwimDistance     float64  `json:"swim_distance"`      // Khoảng cách bơi (mét)
	SurvivalDuration float64  `json:"survival_duration"`  // Thời gian tồn tại (giây)
	WinPlacePerc     *float64 `json:"win_place_perc"`     // Tỷ lệ thứ hạng thắng (0.0 - 1.0)
}

// InvalidRecord đại diện cho bản ghi bị lỗi validation để đẩy vào Dead-Letter Queue (pubg.v1.invalid)
type InvalidRecord struct {
	SourceFile       string            `json:"source_file"`       // Tên file nguồn
	RecordIndex      int64             `json:"record_index"`      // Chỉ số dòng bị lỗi
	RawFields        map[string]string `json:"raw_fields"`        // Dữ liệu thô ban đầu
	ValidationErrors []string          `json:"validation_errors"` // Danh sách chi tiết các lỗi logic/schema
	FailedAt         time.Time         `json:"failed_at"`         // Thời gian ghi nhận lỗi (UTC)
}
