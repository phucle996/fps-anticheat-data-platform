package service_test

import (
	"testing"

	"pubg-anti-cheat/go-ingestor/internal/service"
)

// TestPlayerStatNormalizer_ValidRecord kiểm tra chuẩn hóa bản ghi thô hợp lệ thành EventEnvelope
func TestPlayerStatNormalizer_ValidRecord(t *testing.T) {
	normalizer := service.NewPlayerStatNormalizer("pubg-dataset-test")

	raw := &service.RawRecord{
		SourceFile:  "train_V2.csv",
		RecordIndex: 10,
		Fields: map[string]string{
			"Id":            "player-100",
			"groupId":       "group-200",
			"matchId":       "match-300",
			"kills":         "5",
			"damageDealt":   "450.5",
			"headshotKills": "2",
			"walkDistance":  "1200.0",
			"rideDistance":  "0.0",
			"swimDistance":  "0.0",
			"matchDuration": "900.0",
			"winPlacePerc":  "0.85",
		},
	}

	envelope, invalid, err := normalizer.Normalize(raw)
	if err != nil {
		t.Fatalf("Normalize trả về lỗi bất ngờ: %v", err)
	}
	if invalid != nil {
		t.Fatalf("Bản ghi hợp lệ bị đánh dấu là Invalid: %+v", invalid.ValidationErrors)
	}
	if envelope == nil {
		t.Fatalf("EventEnvelope trả về không được nil")
	}

	if envelope.PlayerID != "player-100" || envelope.MatchID != "match-300" {
		t.Errorf("Mã định danh player_id / match_id sai: %s / %s", envelope.PlayerID, envelope.MatchID)
	}
	if envelope.Payload.Kills != 5 || envelope.Payload.HeadshotKills != 2 {
		t.Errorf("Parse kills / headshot_kills sai: %d / %d", envelope.Payload.Kills, envelope.Payload.HeadshotKills)
	}
	if envelope.EventID == "" {
		t.Errorf("EventID không được để chuỗi rỗng")
	}
}

// TestPlayerStatNormalizer_DeterministicEventID kiểm tra tính bất biến của SHA-256 event_id khi cùng input
func TestPlayerStatNormalizer_DeterministicEventID(t *testing.T) {
	normalizer := service.NewPlayerStatNormalizer("pubg-dataset-test")

	raw := &service.RawRecord{
		SourceFile:  "train_V2.csv",
		RecordIndex: 10,
		Fields: map[string]string{
			"Id":            "player-100",
			"matchId":       "match-300",
			"kills":         "1",
			"damageDealt":   "100.0",
			"headshotKills": "0",
			"walkDistance":  "100.0",
			"rideDistance":  "0.0",
			"swimDistance":  "0.0",
			"matchDuration": "500.0",
		},
	}

	env1, _, _ := normalizer.Normalize(raw)
	env2, _, _ := normalizer.Normalize(raw)

	if env1.EventID != env2.EventID {
		t.Errorf("Deterministic EventID thất bại: %s khác %s", env1.EventID, env2.EventID)
	}
}

// TestPlayerStatNormalizer_SemanticValidationError kiểm tra bắt lỗi khi headshotKills > kills
func TestPlayerStatNormalizer_SemanticValidationError(t *testing.T) {
	normalizer := service.NewPlayerStatNormalizer("pubg-dataset-test")

	raw := &service.RawRecord{
		SourceFile:  "train_V2.csv",
		RecordIndex: 15,
		Fields: map[string]string{
			"Id":            "player-999",
			"matchId":       "match-300",
			"kills":         "2",
			"damageDealt":   "200.0",
			"headshotKills": "5",
			"walkDistance":  "500.0",
			"rideDistance":  "0.0",
			"swimDistance":  "0.0",
			"matchDuration": "600.0",
		},
	}

	envelope, invalid, err := normalizer.Normalize(raw)
	if err != nil {
		t.Fatalf("Normalize không được ném error với invalid record: %v", err)
	}
	if envelope != nil {
		t.Errorf("Envelope phải là nil khi bản ghi vi phạm validation")
	}
	if invalid == nil {
		t.Fatalf("Kỳ vọng trả về InvalidRecord nhưng nhận được nil")
	}
}
