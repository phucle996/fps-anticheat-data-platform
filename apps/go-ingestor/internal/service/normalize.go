package service

import (
	"crypto/sha256"
	"encoding/hex"
	"fmt"
	"strconv"
	"strings"
	"time"

	"pubg-anti-cheat/go-ingestor/internal/contract"
)

// Normalizer định nghĩa interface chuẩn hóa bản ghi thô thành EventEnvelope (*contract.EventEnvelope hoặc *contract.KillEventEnvelope) hoặc InvalidRecord
type Normalizer interface {
	Normalize(raw *RawRecord) (interface{}, *contract.InvalidRecord, error)
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
func (n *PlayerStatNormalizer) Normalize(raw *RawRecord) (interface{}, *contract.InvalidRecord, error) {
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
// Columns: killer_name, match_id, killed_by, killer_placement, killer_position_x, killer_position_y, time, map, victim_name, victim_placement, victim_position_x, victim_position_y
// Trả về contract.KillEventEnvelope chứa đúng 11 trường raw telemetry (nullable pointer fields), không ép kiểu proxy sai nghĩa
func (n *PlayerStatNormalizer) normalizeMatchDeaths(raw *RawRecord) (*contract.KillEventEnvelope, *contract.InvalidRecord, error) {
	var valErrors []string

	matchID := strings.TrimSpace(raw.Fields["match_id"])
	if matchID == "" {
		valErrors = append(valErrors, "thiếu trường bắt buộc 'match_id'")
	}

	// 1. Killer & Victim Names (Nullable)
	var killerNamePtr, victimNamePtr *string
	if kName := strings.TrimSpace(raw.Fields["killer_name"]); kName != "" {
		killerNamePtr = &kName
	}
	if vName := strings.TrimSpace(raw.Fields["victim_name"]); vName != "" {
		victimNamePtr = &vName
	}

	// Ít nhất phải có killer hoặc victim name
	var playerID string
	if killerNamePtr != nil {
		playerID = *killerNamePtr
	} else if victimNamePtr != nil {
		playerID = *victimNamePtr
	} else {
		valErrors = append(valErrors, "thiếu cả 'killer_name' lẫn 'victim_name' — không thể xác định player_id")
	}

	// 2. Killer & Victim Placements (Nullable Int)
	var killerPlacementPtr, victimPlacementPtr *int
	if kpStr := strings.TrimSpace(raw.Fields["killer_placement"]); kpStr != "" {
		if val, err := strconv.Atoi(kpStr); err == nil {
			killerPlacementPtr = &val
		}
	}
	if vpStr := strings.TrimSpace(raw.Fields["victim_placement"]); vpStr != "" {
		if val, err := strconv.Atoi(vpStr); err == nil {
			victimPlacementPtr = &val
		}
	}

	// 3. Killer Position X & Y (Nullable Float)
	var killerXPtr, killerYPtr *float64
	if kxStr := strings.TrimSpace(raw.Fields["killer_position_x"]); kxStr != "" {
		if val, err := parseFloat(kxStr); err == nil {
			killerXPtr = &val
		}
	}
	if kyStr := strings.TrimSpace(raw.Fields["killer_position_y"]); kyStr != "" {
		if val, err := parseFloat(kyStr); err == nil {
			killerYPtr = &val
		}
	}

	// 4. Victim Position X & Y (Nullable Float)
	var victimXPtr, victimYPtr *float64
	if vxStr := strings.TrimSpace(raw.Fields["victim_position_x"]); vxStr != "" {
		if val, err := parseFloat(vxStr); err == nil {
			victimXPtr = &val
		}
	}
	if vyStr := strings.TrimSpace(raw.Fields["victim_position_y"]); vyStr != "" {
		if val, err := parseFloat(vyStr); err == nil {
			victimYPtr = &val
		}
	}

	// 5. Event Time Seconds (field "time" trong CSV, Nullable Float)
	var eventTimePtr *float64
	if tStr := strings.TrimSpace(raw.Fields["time"]); tStr != "" {
		if val, err := parseFloat(tStr); err == nil {
			eventTimePtr = &val
		}
	}

	// 6. Weapon / Killed By (Nullable String)
	var weaponPtr *string
	if wStr := strings.TrimSpace(raw.Fields["killed_by"]); wStr != "" {
		weaponPtr = &wStr
	}

	// Validation rule: killer_name không được trùng với match_id
	if playerID == matchID && playerID != "" {
		valErrors = append(valErrors, "killer_name và match_id không được giống nhau")
	}

	// Trả về InvalidRecord nếu có lỗi validation
	if len(valErrors) > 0 {
		return nil, &contract.InvalidRecord{
			SourceFile:       raw.SourceFile,
			RecordIndex:      raw.RecordIndex,
			RawFields:        raw.Fields,
			ValidationErrors: valErrors,
			FailedAt:         time.Now().UTC(),
		}, nil
	}

	// Sinh Event ID deterministic (SHA-256)
	eventIDStr := fmt.Sprintf("%s:%s:%s:%d", matchID, playerID, raw.SourceFile, raw.RecordIndex)
	hasher := sha256.New()
	hasher.Write([]byte(eventIDStr))
	eventID := hex.EncodeToString(hasher.Sum(nil))

	envelope := &contract.KillEventEnvelope{
		SchemaVersion: "1.0",
		EventID:       eventID,
		Op:            contract.OpKillEventRaw,
		EventTime:     nil,
		IngestTime:    time.Now().UTC(),
		MatchID:       matchID,
		PlayerID:      playerID,
		Source: contract.SourceMetadata{
			Provider:    "kaggle",
			DatasetID:   n.datasetID,
			SchemaType:  string(SchemaMatchDeaths),
			SourceFile:  raw.SourceFile,
			RecordIndex: raw.RecordIndex,
		},
		Payload: contract.KillEventPayload{
			MatchID:          matchID,
			KillerName:       killerNamePtr,
			VictimName:       victimNamePtr,
			KillerPlacement:  killerPlacementPtr,
			VictimPlacement:  victimPlacementPtr,
			KillerPositionX:  killerXPtr,
			KillerPositionY:  killerYPtr,
			VictimPositionX:  victimXPtr,
			VictimPositionY:  victimYPtr,
			EventTimeSeconds: eventTimePtr,
			Weapon:           weaponPtr,
		},
	}

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
