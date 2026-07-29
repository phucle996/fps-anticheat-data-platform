package contract

import (
	"time"
)

// Op constants — định nghĩa tên thao tác phân biệt loại sự kiện theo nguồn gốc dataset
// Downstream (Rust Processor, ML Platform) dùng Op để route và xử lý đúng nghĩa semantic
const (
	// OpMatchSummary — aggregate stats từ dataset finish_placement (catchmeifyoucan/pubg-finish-placement-prediction)
	// Mỗi event = 1 bản tổng kết player trong 1 trận đấu đầy đủ
	// Fields: kills (tổng), damage_dealt (tổng), headshot_kills (tổng), walk/ride/swim distance, survival_duration, win_place_perc
	OpMatchSummary = "data.player_stat.match_summary"

	// OpKillEvent — kill event đơn lẻ từ dataset match_deaths (skihikingkevin/pubg-match-deaths)
	// Mỗi event = 1 lần kill xảy ra trong game, kills luôn = 1
	// Fields: kills=1 (per event), survival_duration (thời điểm kill trong trận), win_place_perc (1/placement)
	// Lưu ý: damage_dealt=0 (không có trong schema), headshot_kills=0 (không biết qua weapon name)
	OpKillEvent = "data.kill_event.kill_death"
)

// EventEnvelope đại diện cho cấu trúc tin nhắn sự kiện hợp lệ gửi vào Kafka Topic (pubg.v1.player-stat.raw)
// Op field là discriminator chính để downstream phân biệt loại event và schema nguồn gốc
type EventEnvelope struct {
	SchemaVersion string            `json:"schema_version"` // Phiên bản schema dữ liệu (mặc định "1.0")
	EventID       string            `json:"event_id"`       // Chuỗi băm SHA-256 duy nhất dùng cho khử trùng lặp
	Op            string            `json:"op"`             // Discriminator: OpMatchSummary hoặc OpKillEvent
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
	DatasetID   string `json:"dataset_id"`   // ID dataset (skihikingkevin-pubg-match-deaths / catchmeifyoucan-pubg-finish-placement-prediction)
	SchemaType  string `json:"schema_type"`  // Schema đã detect: "match_deaths" | "finish_placement" — dùng để downstream tái cấu trúc ngữ nghĩa
	SourceFile  string `json:"source_file"`  // Tên file CSV nguồn (kill_match_stats_final_0.csv / train_V2.csv)
	RecordIndex int64  `json:"record_index"` // Chỉ số dòng bản ghi trong file CSV
}

// PlayerStatPayload định nghĩa các chỉ số thống kê trận đấu của người chơi
// Ngữ nghĩa của từng field phụ thuộc vào Op:
//
//	Op = OpMatchSummary (finish_placement):
//	  Kills            = tổng số mạng trong cả trận
//	  DamageDealt      = tổng sát thương gây ra (damage_dealt)
//	  HeadshotKills    = tổng mạng headshot
//	  WalkDistance     = tổng quãng đường đi bộ (mét)
//	  RideDistance     = tổng quãng đường đi xe (mét)
//	  SwimDistance     = tổng quãng đường bơi (mét)
//	  SurvivalDuration = thời gian sống sót (giây)
//	  WinPlacePerc     = tỷ lệ thứ hạng (0.0 - 1.0, 1.0 = top 1)
//
//	Op = OpKillEvent (match_deaths):
//	  Kills            = 1 (hằng số — mỗi event là 1 kill)
//	  DamageDealt      = 0 (không có trong schema match_deaths)
//	  HeadshotKills    = 0 (không xác định được từ tên vũ khí)
//	  WalkDistance     = magnitude vị trí killer trên map / 1000 (proxy movement)
//	  RideDistance     = 0
//	  SwimDistance     = 0
//	  SurvivalDuration = thời điểm kill xảy ra (giây từ đầu trận, field "time")
//	  WinPlacePerc     = 1 / killer_placement (proxy rank, nullable)
type PlayerStatPayload struct {
	Kills            int64    `json:"kills"`             // Xem comment Op ở trên
	DamageDealt      float64  `json:"damage_dealt"`      // Xem comment Op ở trên
	HeadshotKills    int64    `json:"headshot_kills"`    // Xem comment Op ở trên
	WalkDistance     float64  `json:"walk_distance"`     // Xem comment Op ở trên
	RideDistance     float64  `json:"ride_distance"`     // Xem comment Op ở trên
	SwimDistance     float64  `json:"swim_distance"`     // Xem comment Op ở trên
	SurvivalDuration float64  `json:"survival_duration"` // Xem comment Op ở trên
	WinPlacePerc     *float64 `json:"win_place_perc"`    // Xem comment Op ở trên
}

// InvalidRecord đại diện cho bản ghi bị lỗi validation để đẩy vào Dead-Letter Queue (pubg.v1.invalid)
type InvalidRecord struct {
	SourceFile       string            `json:"source_file"`       // Tên file nguồn
	RecordIndex      int64             `json:"record_index"`      // Chỉ số dòng bị lỗi
	RawFields        map[string]string `json:"raw_fields"`        // Dữ liệu thô ban đầu
	ValidationErrors []string          `json:"validation_errors"` // Danh sách chi tiết các lỗi logic/schema
	FailedAt         time.Time         `json:"failed_at"`         // Thời gian ghi nhận lỗi (UTC)
}
