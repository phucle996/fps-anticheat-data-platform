package pipeline

import (
	"crypto/sha256"
	"encoding/hex"
	"fmt"
	"strconv"
	"strings"
	"time"

	"go-ingestor/internal/contract"
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
func detectSchema(raw *RawRecord) DatasetSchema {
	_, hasKillerName := raw.Fields["killer_name"]
	_, hasVictimName := raw.Fields["victim_name"]
	if hasKillerName || hasVictimName {
		return SchemaMatchDeaths
	}

	_, hasWinPlacePerc := raw.Fields["winPlacePerc"]
	_, hasId := raw.Fields["Id"]
	if hasWinPlacePerc || hasId {
		return SchemaFinishPlacement
	}

	return SchemaUnknown
}

// Normalize chuyển đổi bản ghi thô thành EventEnvelope hoặc KillEventEnvelope (nếu hợp lệ) hoặc InvalidRecord (nếu không hợp lệ)
func (n *PlayerStatNormalizer) Normalize(raw *RawRecord) (interface{}, *contract.InvalidRecord, error) {
	if raw == nil || len(raw.Fields) == 0 {
		return nil, &contract.InvalidRecord{
			SourceFile:       getRawSourceFile(raw),
			RecordIndex:      getRawRecordIndex(raw),
			RawFields:        make(map[string]string),
			ValidationErrors: []string{"bản ghi thô rỗng hoặc nil"},
			FailedAt:         time.Now().UTC(),
		}, nil
	}

	schema := detectSchema(raw)
	if schema == SchemaMatchDeaths {
		return n.normalizeMatchDeaths(raw)
	}

	return n.normalizeFinishPlacement(raw)
}

// normalizeMatchDeaths chuẩn hóa bản ghi từ dataset pubg-match-deaths
func (n *PlayerStatNormalizer) normalizeMatchDeaths(raw *RawRecord) (interface{}, *contract.InvalidRecord, error) {
	var validationErrs []string

	matchID := strings.TrimSpace(raw.Fields["match_id"])
	if matchID == "" {
		validationErrs = append(validationErrs, "match_id không được để rỗng")
	}

	killerNameStr := strings.TrimSpace(raw.Fields["killer_name"])
	victimNameStr := strings.TrimSpace(raw.Fields["victim_name"])

	if killerNameStr == "" && victimNameStr == "" {
		validationErrs = append(validationErrs, "cả killer_name và victim_name đều rỗng")
	}

	var killerName *string
	if killerNameStr != "" {
		killerName = &killerNameStr
	}

	var victimName *string
	if victimNameStr != "" {
		victimName = &victimNameStr
	}

	var killerPlacement *int
	if str, exists := raw.Fields["killer_placement"]; exists && strings.TrimSpace(str) != "" {
		if val, err := strconv.Atoi(strings.TrimSpace(str)); err == nil {
			killerPlacement = &val
		} else {
			validationErrs = append(validationErrs, fmt.Sprintf("killer_placement không hợp lệ: %s", str))
		}
	}

	var victimPlacement *int
	if str, exists := raw.Fields["victim_placement"]; exists && strings.TrimSpace(str) != "" {
		if val, err := strconv.Atoi(strings.TrimSpace(str)); err == nil {
			victimPlacement = &val
		} else {
			validationErrs = append(validationErrs, fmt.Sprintf("victim_placement không hợp lệ: %s", str))
		}
	}

	var killerPosX *float64
	if str, exists := raw.Fields["killer_position_x"]; exists && strings.TrimSpace(str) != "" {
		if val, err := strconv.ParseFloat(strings.TrimSpace(str), 64); err == nil {
			killerPosX = &val
		} else {
			validationErrs = append(validationErrs, fmt.Sprintf("killer_position_x không hợp lệ: %s", str))
		}
	}

	var killerPosY *float64
	if str, exists := raw.Fields["killer_position_y"]; exists && strings.TrimSpace(str) != "" {
		if val, err := strconv.ParseFloat(strings.TrimSpace(str), 64); err == nil {
			killerPosY = &val
		} else {
			validationErrs = append(validationErrs, fmt.Sprintf("killer_position_y không hợp lệ: %s", str))
		}
	}

	var victimPosX *float64
	if str, exists := raw.Fields["victim_position_x"]; exists && strings.TrimSpace(str) != "" {
		if val, err := strconv.ParseFloat(strings.TrimSpace(str), 64); err == nil {
			victimPosX = &val
		} else {
			validationErrs = append(validationErrs, fmt.Sprintf("victim_position_x không hợp lệ: %s", str))
		}
	}

	var victimPosY *float64
	if str, exists := raw.Fields["victim_position_y"]; exists && strings.TrimSpace(str) != "" {
		if val, err := strconv.ParseFloat(strings.TrimSpace(str), 64); err == nil {
			victimPosY = &val
		} else {
			validationErrs = append(validationErrs, fmt.Sprintf("victim_position_y không hợp lệ: %s", str))
		}
	}

	var eventTimeSec *float64
	if str, exists := raw.Fields["time"]; exists && strings.TrimSpace(str) != "" {
		if val, err := strconv.ParseFloat(strings.TrimSpace(str), 64); err == nil {
			if val < 0 {
				validationErrs = append(validationErrs, fmt.Sprintf("time âm: %.2f", val))
			} else {
				eventTimeSec = &val
			}
		} else {
			validationErrs = append(validationErrs, fmt.Sprintf("time không hợp lệ: %s", str))
		}
	}

	var weapon *string
	if str, exists := raw.Fields["killed_by"]; exists && strings.TrimSpace(str) != "" {
		w := strings.TrimSpace(str)
		weapon = &w
	}

	if len(validationErrs) > 0 {
		return nil, &contract.InvalidRecord{
			SourceFile:       raw.SourceFile,
			RecordIndex:      raw.RecordIndex,
			RawFields:        raw.Fields,
			ValidationErrors: validationErrs,
			FailedAt:         time.Now().UTC(),
		}, nil
	}

	playerIDRaw := killerNameStr
	if playerIDRaw == "" {
		playerIDRaw = victimNameStr
	}
	playerIDHash := hashSHA256(playerIDRaw)
	matchIDHash := hashSHA256(matchID)

	eventIDRaw := fmt.Sprintf("%s:%s:%d:%s:%s",
		raw.SourceFile, matchID, raw.RecordIndex, playerIDRaw, victimNameStr)
	eventID := hashSHA256(eventIDRaw)

	payload := contract.KillEventPayload{
		MatchID:          matchIDHash,
		KillerName:       killerName,
		VictimName:       victimName,
		KillerPlacement:  killerPlacement,
		VictimPlacement:  victimPlacement,
		KillerPositionX:  killerPosX,
		KillerPositionY:  killerPosY,
		VictimPositionX:  victimPosX,
		VictimPositionY:  victimPosY,
		EventTimeSeconds: eventTimeSec,
		Weapon:           weapon,
	}

	envelope := &contract.KillEventEnvelope{
		SchemaVersion: "1.0",
		EventID:       eventID,
		Op:            contract.OpKillEventRaw,
		EventTime:     nil,
		IngestTime:    time.Now().UTC(),
		MatchID:       matchIDHash,
		PlayerID:      playerIDHash,
		Source: contract.SourceMetadata{
			Provider:    "kaggle",
			DatasetID:   n.datasetID,
			SchemaType:  string(SchemaMatchDeaths),
			SourceFile:  raw.SourceFile,
			RecordIndex: raw.RecordIndex,
		},
		Payload: payload,
	}

	return envelope, nil, nil
}

// normalizeFinishPlacement chuẩn hóa bản ghi từ dataset pubg-finish-placement-prediction
func (n *PlayerStatNormalizer) normalizeFinishPlacement(raw *RawRecord) (interface{}, *contract.InvalidRecord, error) {
	var validationErrs []string

	matchID := strings.TrimSpace(raw.Fields["matchId"])
	if matchID == "" {
		validationErrs = append(validationErrs, "matchId không được để rỗng")
	}

	playerID := strings.TrimSpace(raw.Fields["Id"])
	if playerID == "" {
		validationErrs = append(validationErrs, "Id (player_id) không được để rỗng")
	}

	kills, err := parseGroupedInt(raw.Fields, "kills")
	if err != nil {
		validationErrs = append(validationErrs, fmt.Sprintf("kills không hợp lệ: %v", err))
	} else if kills < 0 {
		validationErrs = append(validationErrs, fmt.Sprintf("kills không được âm: %d", kills))
	}

	headshotKills, err := parseGroupedInt(raw.Fields, "headshotKills")
	if err != nil {
		validationErrs = append(validationErrs, fmt.Sprintf("headshotKills không hợp lệ: %v", err))
	} else if headshotKills < 0 {
		validationErrs = append(validationErrs, fmt.Sprintf("headshotKills không được âm: %d", headshotKills))
	} else if headshotKills > kills {
		validationErrs = append(validationErrs, fmt.Sprintf("headshotKills (%d) lớn hơn tổng kills (%d)", headshotKills, kills))
	}

	damageDealt, err := parseGroupedFloat(raw.Fields, "damageDealt")
	if err != nil {
		validationErrs = append(validationErrs, fmt.Sprintf("damageDealt không hợp lệ: %v", err))
	} else if damageDealt < 0 {
		validationErrs = append(validationErrs, fmt.Sprintf("damageDealt không được âm: %.2f", damageDealt))
	}

	walkDistance, err := parseGroupedFloat(raw.Fields, "walkDistance")
	if err != nil {
		validationErrs = append(validationErrs, fmt.Sprintf("walkDistance không hợp lệ: %v", err))
	} else if walkDistance < 0 {
		validationErrs = append(validationErrs, fmt.Sprintf("walkDistance không được âm: %.2f", walkDistance))
	}

	rideDistance, err := parseGroupedFloat(raw.Fields, "rideDistance")
	if err != nil {
		validationErrs = append(validationErrs, fmt.Sprintf("rideDistance không hợp lệ: %v", err))
	} else if rideDistance < 0 {
		validationErrs = append(validationErrs, fmt.Sprintf("rideDistance không được âm: %.2f", rideDistance))
	}

	swimDistance, err := parseGroupedFloat(raw.Fields, "swimDistance")
	if err != nil {
		validationErrs = append(validationErrs, fmt.Sprintf("swimDistance không hợp lệ: %v", err))
	} else if swimDistance < 0 {
		validationErrs = append(validationErrs, fmt.Sprintf("swimDistance không được âm: %.2f", swimDistance))
	}

	survivalDuration, err := parseGroupedFloat(raw.Fields, "matchDuration")
	if err != nil {
		validationErrs = append(validationErrs, fmt.Sprintf("matchDuration không hợp lệ: %v", err))
	} else if survivalDuration < 0 {
		validationErrs = append(validationErrs, fmt.Sprintf("matchDuration không được âm: %.2f", survivalDuration))
	}

	var winPlacePercPtr *float64
	winPlaceStr, hasWinPlace := raw.Fields["winPlacePerc"]
	if hasWinPlace && strings.TrimSpace(winPlaceStr) != "" {
		val, err := strconv.ParseFloat(strings.TrimSpace(winPlaceStr), 64)
		if err != nil {
			validationErrs = append(validationErrs, fmt.Sprintf("winPlacePerc không hợp lệ: %s", winPlaceStr))
		} else if val < 0.0 || val > 1.0 {
			validationErrs = append(validationErrs, fmt.Sprintf("winPlacePerc nằm ngoài khoảng [0.0, 1.0]: %.4f", val))
		} else {
			winPlacePercPtr = &val
		}
	}

	if len(validationErrs) > 0 {
		return nil, &contract.InvalidRecord{
			SourceFile:       raw.SourceFile,
			RecordIndex:      raw.RecordIndex,
			RawFields:        raw.Fields,
			ValidationErrors: validationErrs,
			FailedAt:         time.Now().UTC(),
		}, nil
	}

	playerIDHash := hashSHA256(playerID)
	matchIDHash := hashSHA256(matchID)

	eventIDRaw := fmt.Sprintf("%s:%s:%d:%s", raw.SourceFile, matchID, raw.RecordIndex, playerID)
	eventID := hashSHA256(eventIDRaw)

	payload := contract.PlayerStatPayload{
		Kills:            kills,
		DamageDealt:      damageDealt,
		HeadshotKills:    headshotKills,
		WalkDistance:     walkDistance,
		RideDistance:     rideDistance,
		SwimDistance:     swimDistance,
		SurvivalDuration: survivalDuration,
		WinPlacePerc:     winPlacePercPtr,
	}

	envelope := &contract.EventEnvelope{
		SchemaVersion: "1.0",
		EventID:       eventID,
		Op:            contract.OpMatchSummary,
		EventTime:     nil,
		IngestTime:    time.Now().UTC(),
		MatchID:       matchIDHash,
		PlayerID:      playerIDHash,
		Source: contract.SourceMetadata{
			Provider:    "kaggle",
			DatasetID:   n.datasetID,
			SchemaType:  string(SchemaFinishPlacement),
			SourceFile:  raw.SourceFile,
			RecordIndex: raw.RecordIndex,
		},
		Payload: payload,
	}

	return envelope, nil, nil
}

// Helper SHA-256 Hashing an toàn
func hashSHA256(input string) string {
	sum := sha256.Sum256([]byte(input))
	return hex.EncodeToString(sum[:])
}

func parseGroupedInt(fields map[string]string, key string) (int64, error) {
	valStr, exists := fields[key]
	if !exists || strings.TrimSpace(valStr) == "" {
		return 0, nil
	}
	val, err := strconv.ParseInt(strings.TrimSpace(valStr), 10, 64)
	if err != nil {
		return 0, err
	}
	return val, nil
}

func parseGroupedFloat(fields map[string]string, key string) (float64, error) {
	valStr, exists := fields[key]
	if !exists || strings.TrimSpace(valStr) == "" {
		return 0, nil
	}
	val, err := strconv.ParseFloat(strings.TrimSpace(valStr), 64)
	if err != nil {
		return 0, err
	}
	return val, nil
}

func getRawSourceFile(raw *RawRecord) string {
	if raw != nil {
		return raw.SourceFile
	}
	return ""
}

func getRawRecordIndex(raw *RawRecord) int64 {
	if raw != nil {
		return raw.RecordIndex
	}
	return 0
}

// Compile-time interface assertion
var _ Normalizer = (*PlayerStatNormalizer)(nil)
