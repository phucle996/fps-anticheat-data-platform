package service

import (
	"crypto/sha256"
	"encoding/hex"
	"fmt"
	"math"
	"strconv"
	"strings"
	"time"

	"pubg-anti-cheat/go-ingestor/internal/contract"
)

// Normalizer định nghĩa interface chuẩn hóa bản ghi thô thành EventEnvelope hoặc InvalidRecord
type Normalizer interface {
	Normalize(raw *RawRecord) (*contract.EventEnvelope, *contract.InvalidRecord, error)
}

// DatasetSchema định nghĩa 2 schema dataset được hỗ trợ
type DatasetSchema string

const (
	// SchemaFinishPlacement — catchmeifyoucan/pubg-finish-placement-prediction
	// Columns: Id, matchId, kills, headshotKills, damageDealt, walkDistance, rideDistance, swimDistance, matchDuration, winPlacePerc
	SchemaFinishPlacement DatasetSchema = "finish_placement"

	// SchemaMatchDeaths — skihikingkevin/pubg-match-deaths (kill_match_stats_final_*.csv)
	// Columns: killer_name, match_id, killed_by, killer_placement, killer_position_x, killer_position_y, time, map
	SchemaMatchDeaths DatasetSchema = "match_deaths"

	// SchemaUnknown — không nhận dạng được, fallback về finish_placement
	SchemaUnknown DatasetSchema = "unknown"

	// ContractOpMatchSummary và ContractOpKillEvent re-export từ contract package
	// để test file (package service_test) truy cập mà không cần import contract trực tiếp
	ContractOpMatchSummary = contract.OpMatchSummary
	ContractOpKillEvent    = contract.OpKillEvent
)

// PlayerStatNormalizer thực thi interface Normalizer cho bản ghi PUBG Player Statistics
// Hỗ trợ tự động phát hiện schema từ header CSV (Schema Auto-Detection)
// Thread-safe: không lưu trạng thái mutable — detect stateless mỗi lần (2 map lookup O(1))
type PlayerStatNormalizer struct {
	datasetID string // ID dataset phục vụ metadata nguồn
}

// NewPlayerStatNormalizer khởi tạo PlayerStatNormalizer
func NewPlayerStatNormalizer(datasetID string) *PlayerStatNormalizer {
	return &PlayerStatNormalizer{
		datasetID: datasetID,
	}
}

// detectSchema phân tích các trường có trong bản ghi và xác định schema dataset
// Thread-safe: pure function, không đụng vào state mà chỉ đọc raw.Fields (read-only)
// Mỗi gọ đếu chỉ làm đúng 2 map lookup O(1) — không có race condition
func detectSchema(raw *RawRecord) DatasetSchema {
	// Kiểm tra dấu hiệu schema match_deaths: có trường killer_name và victim_name
	_, hasKillerName := raw.Fields["killer_name"]
	_, hasVictimName := raw.Fields["victim_name"]
	if hasKillerName && hasVictimName {
		return SchemaMatchDeaths
	}

	// Kiểm tra dấu hiệu schema finish_placement: có trường Id và matchId
	_, hasId := raw.Fields["Id"]
	_, hasMatchId := raw.Fields["matchId"]
	if hasId && hasMatchId {
		return SchemaFinishPlacement
	}

	// Fallback: thử finish_placement nếu không nhận dạng được
	return SchemaFinishPlacement
}

// Normalize thực thi luồng parse, sinh event_id định hạn SHA-256 và validate dữ liệu
// Thread-safe: detectSchema là pure function stateless, không có shared mutable state
func (n *PlayerStatNormalizer) Normalize(raw *RawRecord) (*contract.EventEnvelope, *contract.InvalidRecord, error) {
	schema := detectSchema(raw) // Gọi package-level function, không đụng state

	switch schema {
	case SchemaMatchDeaths:
		return n.normalizeMatchDeaths(raw)
	default:
		// SchemaFinishPlacement và fallback đều dùng logic cũ
		return n.normalizeFinishPlacement(raw)
	}
}

// normalizeFinishPlacement xử lý dataset catchmeifyoucan/pubg-finish-placement-prediction
// Columns: Id, matchId, kills, headshotKills, damageDealt, walkDistance, rideDistance, swimDistance, matchDuration, winPlacePerc
func (n *PlayerStatNormalizer) normalizeFinishPlacement(raw *RawRecord) (*contract.EventEnvelope, *contract.InvalidRecord, error) {
	var valErrors []string

	// 1. Trích xuất các trường định danh cơ bản
	playerID := strings.TrimSpace(raw.Fields["Id"])
	matchID := strings.TrimSpace(raw.Fields["matchId"])

	if playerID == "" {
		valErrors = append(valErrors, "thiếu trường bắt buộc 'Id' (player_id)")
	}
	if matchID == "" {
		valErrors = append(valErrors, "thiếu trường bắt buộc 'matchId'")
	}

	// 2. Parse các trường kiểu số nguyên (Integer)
	kills, errK := parseInteger(raw.Fields["kills"])
	if errK != nil {
		valErrors = append(valErrors, fmt.Sprintf("lỗi parse 'kills': %v", errK))
	}

	headshotKills, errHK := parseInteger(raw.Fields["headshotKills"])
	if errHK != nil {
		valErrors = append(valErrors, fmt.Sprintf("lỗi parse 'headshotKills': %v", errHK))
	}

	// 3. Parse các trường kiểu số thực (Float)
	damageDealt, errD := parseFloat(raw.Fields["damageDealt"])
	if errD != nil {
		valErrors = append(valErrors, fmt.Sprintf("lỗi parse 'damageDealt': %v", errD))
	}

	walkDistance, errW := parseFloat(raw.Fields["walkDistance"])
	if errW != nil {
		valErrors = append(valErrors, fmt.Sprintf("lỗi parse 'walkDistance': %v", errW))
	}

	rideDistance, errR := parseFloat(raw.Fields["rideDistance"])
	if errR != nil {
		valErrors = append(valErrors, fmt.Sprintf("lỗi parse 'rideDistance': %v", errR))
	}

	swimDistance, errS := parseFloat(raw.Fields["swimDistance"])
	if errS != nil {
		valErrors = append(valErrors, fmt.Sprintf("lỗi parse 'swimDistance': %v", errS))
	}

	survivalDuration, errM := parseFloat(raw.Fields["matchDuration"])
	if errM != nil {
		valErrors = append(valErrors, fmt.Sprintf("lỗi parse 'matchDuration': %v", errM))
	}

	var winPlacePercPtr *float64
	if strVal, ok := raw.Fields["winPlacePerc"]; ok && strings.TrimSpace(strVal) != "" {
		val, errWP := parseFloat(strVal)
		if errWP != nil {
			valErrors = append(valErrors, fmt.Sprintf("lỗi parse 'winPlacePerc': %v", errWP))
		} else {
			winPlacePercPtr = &val
		}
	}

	// 4. Semantic Validation Rules
	if errK == nil && kills < 0 {
		valErrors = append(valErrors, fmt.Sprintf("kills (%d) không được âm", kills))
	}
	if errHK == nil && headshotKills < 0 {
		valErrors = append(valErrors, fmt.Sprintf("headshotKills (%d) không được âm", headshotKills))
	}
	if errK == nil && errHK == nil && headshotKills > kills {
		valErrors = append(valErrors, fmt.Sprintf("headshotKills (%d) không được lớn hơn kills (%d)", headshotKills, kills))
	}
	if errD == nil && damageDealt < 0 {
		valErrors = append(valErrors, fmt.Sprintf("damageDealt (%.2f) không được âm", damageDealt))
	}
	if errW == nil && walkDistance < 0 {
		valErrors = append(valErrors, fmt.Sprintf("walkDistance (%.2f) không được âm", walkDistance))
	}

	// 5. Trả về InvalidRecord nếu có lỗi
	if len(valErrors) > 0 {
		return nil, &contract.InvalidRecord{
			SourceFile:       raw.SourceFile,
			RecordIndex:      raw.RecordIndex,
			RawFields:        raw.Fields,
			ValidationErrors: valErrors,
			FailedAt:         time.Now().UTC(),
		}, nil
	}

	// 6. Đóng gói EventEnvelope với Op = OpMatchSummary (aggregate match stats)
	envelope := n.buildEnvelope(
		contract.OpMatchSummary, string(SchemaFinishPlacement),
		matchID, playerID, raw,
		kills, headshotKills, damageDealt,
		walkDistance, rideDistance, swimDistance, survivalDuration, winPlacePercPtr,
	)

	return envelope, nil, nil
}

// normalizeMatchDeaths xử lý dataset skihikingkevin/pubg-match-deaths (kill_match_stats_final_*.csv)
// Columns: killer_name, match_id, killed_by, killer_placement, killer_position_x, killer_position_y, time, map, victim_name, victim_placement
// Mỗi dòng = 1 kill event của killer — ánh xạ sang PlayerStatPayload theo logic kill event
func (n *PlayerStatNormalizer) normalizeMatchDeaths(raw *RawRecord) (*contract.EventEnvelope, *contract.InvalidRecord, error) {
	var valErrors []string

	// 1. Trích xuất định danh từ schema match_deaths
	// killer_name dùng làm player_id — đại diện cho người chơi thực hiện kill
	// Nếu killer_name rỗng → player chết do môi trường (BlueZone/RedZone/Bleeding/Explosion)
	// Trong trường hợp này, dùng victim_name làm player_id để track nạn nhân
	killerName := strings.TrimSpace(raw.Fields["killer_name"])
	victimName := strings.TrimSpace(raw.Fields["victim_name"])
	matchID := strings.TrimSpace(raw.Fields["match_id"])

	var playerID string
	var killsPerRecord int64
	if killerName != "" {
		// Normal kill: killer là người chơi
		playerID = killerName
		killsPerRecord = 1
	} else {
		// Environmental death (BlueZone, RedZone, Bleeding, ...): track theo victim
		// Kills = 0 vì đây không phải kill bởi người chơi
		playerID = victimName
		killsPerRecord = 0
	}

	if playerID == "" {
		valErrors = append(valErrors, "thiếu cả 'killer_name' lẫn 'victim_name' — không thể xác định player_id")
	}
	if matchID == "" {
		valErrors = append(valErrors, "thiếu trường bắt buộc 'match_id'")
	}

	// 2. Mỗi dòng = 1 kill event → Kills=1, HeadshotKills cần kiểm tra thêm
	// killed_by chứa vũ khí: nếu chứa "head" hoặc một số weapon như "VSS" thì không đủ data cho headshot
	// Vì schema không có trường headshot trực tiếp → HeadshotKills=0 (conservative)
	const headshotKillsPerRecord int64 = 0

	// 3. Tính walkDistance từ vị trí killer (Euclidean distance từ origin)
	// Đây là vị trí trên map, dùng làm proxy cho movement distance
	posX, errX := parseFloat(raw.Fields["killer_position_x"])
	posY, errY := parseFloat(raw.Fields["killer_position_y"])
	var walkDistance float64
	if errX == nil && errY == nil {
		// Magnitude của vector vị trí — đại diện cho khoảng cách từ trung tâm map
		walkDistance = math.Sqrt(posX*posX+posY*posY) / 1000.0 // Scale từ game units về mét ước tính
	}

	// 4. Parse survival time từ field "time" (giây trong trận)
	survivalDuration, errT := parseFloat(raw.Fields["time"])
	if errT != nil {
		// time không bắt buộc — fallback về 0
		survivalDuration = 0
	}

	// 5. Parse placement thành winPlacePerc: killer_placement=1 → 1.0 (tốt nhất), cao hơn → nhỏ hơn
	var winPlacePercPtr *float64
	if placementStr, ok := raw.Fields["killer_placement"]; ok && strings.TrimSpace(placementStr) != "" {
		placement, errP := parseFloat(placementStr)
		if errP == nil && placement > 0 {
			// Normalize placement: placement 1 = top rank → winPlacePerc gần 1.0
			// Dùng 1/placement làm proxy (không biết tổng số người chơi)
			val := 1.0 / placement
			winPlacePercPtr = &val
		}
	}

	// 6. DamageDealt — schema không có trường này → dùng 0 (data limitation)
	// Trong kill event, damage implied là >= 100 (đủ để kill) nhưng không có exact value
	const damageDealtProxy float64 = 0

	// 7. Semantic validation cho match_deaths schema
	if playerID == matchID && playerID != "" {
		valErrors = append(valErrors, "killer_name và match_id không được giống nhau")
	}

	// 8. Trả về InvalidRecord nếu có lỗi validation
	if len(valErrors) > 0 {
		return nil, &contract.InvalidRecord{
			SourceFile:       raw.SourceFile,
			RecordIndex:      raw.RecordIndex,
			RawFields:        raw.Fields,
			ValidationErrors: valErrors,
			FailedAt:         time.Now().UTC(),
		}, nil
	}

	// 9. Đóng gói EventEnvelope với Op = OpKillEvent (per-kill event)
	envelope := n.buildEnvelope(
		contract.OpKillEvent, string(SchemaMatchDeaths),
		matchID, playerID, raw,
		killsPerRecord, headshotKillsPerRecord, damageDealtProxy,
		walkDistance, 0, 0, survivalDuration, winPlacePercPtr,
	)

	return envelope, nil, nil
}

// buildEnvelope tạo EventEnvelope chuẩn hóa từ các field đã parse
// op và schemaType là discriminator để downstream nhận biết nguồn gốc và ngữ nghĩa của payload
func (n *PlayerStatNormalizer) buildEnvelope(
	op, schemaType string,
	matchID, playerID string,
	raw *RawRecord,
	kills, headshotKills int64,
	damageDealt, walkDistance, rideDistance, swimDistance, survivalDuration float64,
	winPlacePerc *float64,
) *contract.EventEnvelope {
	// Sinh mã băm Deterministic Event ID (SHA-256) — idempotent, dedup-safe
	eventIDStr := fmt.Sprintf("%s:%s:%s:%d", matchID, playerID, raw.SourceFile, raw.RecordIndex)
	hasher := sha256.New()
	hasher.Write([]byte(eventIDStr))
	eventID := hex.EncodeToString(hasher.Sum(nil))

	return &contract.EventEnvelope{
		SchemaVersion: "1.0",
		EventID:       eventID,
		Op:            op, // Discriminator: OpMatchSummary hoặc OpKillEvent
		EventTime:     nil,
		IngestTime:    time.Now().UTC(),
		MatchID:       matchID,
		PlayerID:      playerID,
		Source: contract.SourceMetadata{
			Provider:    "kaggle",
			DatasetID:   n.datasetID,
			SchemaType:  schemaType, // Tag rõ schema đã detect để downstream tái cấu trúc ngữ nghĩa
			SourceFile:  raw.SourceFile,
			RecordIndex: raw.RecordIndex,
		},
		Payload: contract.PlayerStatPayload{
			Kills:            kills,
			DamageDealt:      damageDealt,
			HeadshotKills:    headshotKills,
			WalkDistance:     walkDistance,
			RideDistance:     rideDistance,
			SwimDistance:     swimDistance,
			SurvivalDuration: survivalDuration,
			WinPlacePerc:     winPlacePerc,
		},
	}
}

func parseInteger(valStr string) (int64, error) {
	valStr = strings.TrimSpace(valStr)
	if valStr == "" {
		return 0, nil
	}
	if floatVal, err := strconv.ParseFloat(valStr, 64); err == nil {
		return int64(floatVal), nil
	}
	return strconv.ParseInt(valStr, 10, 64)
}

func parseFloat(valStr string) (float64, error) {
	valStr = strings.TrimSpace(valStr)
	if valStr == "" {
		return 0.0, nil
	}
	return strconv.ParseFloat(valStr, 64)
}
