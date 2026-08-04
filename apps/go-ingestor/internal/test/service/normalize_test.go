package service_test

import (
	"testing"

	"go-ingestor/internal/contract"
	"go-ingestor/internal/pipeline"
)

// TestPlayerStatNormalizer_ValidRecord kiểm tra chuẩn hóa bản ghi thô hợp lệ thành EventEnvelope
// Schema: finish_placement (Id, matchId, kills, headshotKills, ...)
func TestPlayerStatNormalizer_ValidRecord(t *testing.T) {
	normalizer := pipeline.NewPlayerStatNormalizer("pubg-dataset-test")

	raw := &pipeline.RawRecord{
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

	result, invalid, err := normalizer.Normalize(raw)
	if err != nil {
		t.Fatalf("Normalize trả về lỗi bất ngờ: %v", err)
	}
	if invalid != nil {
		t.Fatalf("Bản ghi hợp lệ bị đánh dấu là Invalid: %+v", invalid.ValidationErrors)
	}
	if result == nil {
		t.Fatalf("EventEnvelope trả về không được nil")
	}

	envelope, ok := result.(*contract.EventEnvelope)
	if !ok {
		t.Fatalf("Kỳ vọng trả về *contract.EventEnvelope cho schema finish_placement")
	}

	// Chú ý: PlayerID và MatchID đã được băm SHA-256 trong normalizer
	if envelope.Payload.Kills != 5 || envelope.Payload.HeadshotKills != 2 {
		t.Errorf("Parse kills / headshot_kills sai: %d / %d", envelope.Payload.Kills, envelope.Payload.HeadshotKills)
	}
	if envelope.Op != contract.OpMatchSummary {
		t.Errorf("Op sai: %s, kỳ vọng: %s", envelope.Op, contract.OpMatchSummary)
	}
	if envelope.Source.SchemaType != "finish_placement" {
		t.Errorf("SchemaType sai: %s, kỳ vọng: finish_placement", envelope.Source.SchemaType)
	}
	if envelope.EventID == "" {
		t.Errorf("EventID không được để chuỗi rỗng")
	}
}

// TestPlayerStatNormalizer_DeterministicEventID kiểm tra tính bất biến của SHA-256 event_id khi cùng input
func TestPlayerStatNormalizer_DeterministicEventID(t *testing.T) {
	normalizer := pipeline.NewPlayerStatNormalizer("pubg-dataset-test")

	raw := &pipeline.RawRecord{
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

	res1, _, _ := normalizer.Normalize(raw)
	res2, _, _ := normalizer.Normalize(raw)

	env1 := res1.(*contract.EventEnvelope)
	env2 := res2.(*contract.EventEnvelope)

	if env1.EventID != env2.EventID {
		t.Errorf("Deterministic EventID thất bại: %s khác %s", env1.EventID, env2.EventID)
	}
}

// TestPlayerStatNormalizer_MatchDeaths_ValidRecord kiểm tra chuẩn hóa kill event từ kill_match_stats_final_*.csv
func TestPlayerStatNormalizer_MatchDeaths_ValidRecord(t *testing.T) {
	normalizer := pipeline.NewPlayerStatNormalizer("skihikingkevin-pubg-match-deaths")

	raw := &pipeline.RawRecord{
		SourceFile:  "kill_match_stats_final_0.csv",
		RecordIndex: 1,
		Fields: map[string]string{
			"killed_by":          "SCAR-L",
			"killer_name":        "SniperPro99",
			"killer_placement":   "4",
			"killer_position_x":  "657725.1",
			"killer_position_y":  "146275.2",
			"map":                "MIRAMAR",
			"match_id":           "abc123matchXYZ",
			"time":               "823",
			"victim_name":        "VictimPlayer1",
			"victim_placement":   "5",
			"victim_position_x":  "657000.0",
			"victim_position_y":  "146000.0",
		},
	}

	result, invalid, err := normalizer.Normalize(raw)
	if err != nil {
		t.Fatalf("Normalize trả về lỗi bất ngờ: %v", err)
	}
	if invalid != nil {
		t.Fatalf("Kill event hợp lệ bị đánh dấu là Invalid: %+v", invalid.ValidationErrors)
	}
	if result == nil {
		t.Fatalf("KillEventEnvelope không được nil với kill event hợp lệ")
	}

	envelope, ok := result.(*contract.KillEventEnvelope)
	if !ok {
		t.Fatalf("Kỳ vọng trả về *contract.KillEventEnvelope cho schema match_deaths")
	}

	if envelope.Payload.KillerName == nil || *envelope.Payload.KillerName != "SniperPro99" {
		t.Errorf("KillerName sai")
	}
	if envelope.Payload.Weapon == nil || *envelope.Payload.Weapon != "SCAR-L" {
		t.Errorf("Weapon sai")
	}
	if envelope.Op != contract.OpKillEventRaw {
		t.Errorf("Op sai: %s, kỳ vọng: %s", envelope.Op, contract.OpKillEventRaw)
	}
}
