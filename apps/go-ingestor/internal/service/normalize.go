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

// Normalizer định nghĩa interface chuẩn hóa bản ghi thô thành EventEnvelope hoặc InvalidRecord
type Normalizer interface {
	Normalize(raw *RawRecord) (*contract.EventEnvelope, *contract.InvalidRecord, error)
}

// PlayerStatNormalizer thực thi interface Normalizer cho bản ghi PUBG Player Statistics
type PlayerStatNormalizer struct {
	datasetID string // ID dataset phục vụ metadata nguồn
}

// NewPlayerStatNormalizer khởi tạo PlayerStatNormalizer
func NewPlayerStatNormalizer(datasetID string) *PlayerStatNormalizer {
	return &PlayerStatNormalizer{
		datasetID: datasetID,
	}
}

// Normalize thực thi luồng parse, sinh event_id định hạn SHA-256 và validate dữ liệu
func (n *PlayerStatNormalizer) Normalize(raw *RawRecord) (*contract.EventEnvelope, *contract.InvalidRecord, error) {
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

	// 6. Sinh mã băm Deterministic Event ID (SHA-256)
	eventIDStr := fmt.Sprintf("%s:%s:%s:%d", matchID, playerID, raw.SourceFile, raw.RecordIndex)
	hasher := sha256.New()
	hasher.Write([]byte(eventIDStr))
	eventID := hex.EncodeToString(hasher.Sum(nil))

	// 7. Đóng gói EventEnvelope thành công
	envelope := &contract.EventEnvelope{
		SchemaVersion: "1.0",
		EventID:       eventID,
		Op:            "data.player_stat.match_summary",
		EventTime:     nil,
		IngestTime:    time.Now().UTC(),
		MatchID:       matchID,
		PlayerID:      playerID,
		Source: contract.SourceMetadata{
			Provider:    "kaggle",
			DatasetID:   n.datasetID,
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
			WinPlacePerc:     winPlacePercPtr,
		},
	}

	return envelope, nil, nil
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
