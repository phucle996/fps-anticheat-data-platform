package service_test

import (
	"testing"

	"pubg-anti-cheat/go-ingestor/internal/service"
)

// TestPlayerStatNormalizer_ValidRecord kiểm tra chuẩn hóa bản ghi thô hợp lệ thành EventEnvelope
// Schema: finish_placement (Id, matchId, kills, headshotKills, ...)
func TestPlayerStatNormalizer_ValidRecord(t *testing.T) {
	// Tạo normalizer mới cho mỗi test — tránh cache schema giữa các test cases
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
	// Verify Op discriminator — phải là OpMatchSummary cho schema finish_placement
	if envelope.Op != service.ContractOpMatchSummary {
		t.Errorf("Op sai: %s, kỳ vọng: %s", envelope.Op, service.ContractOpMatchSummary)
	}
	// Verify SchemaType — phải tag đúng schema nguồn gốc
	if envelope.Source.SchemaType != "finish_placement" {
		t.Errorf("SchemaType sai: %s, kỳ vọng: finish_placement", envelope.Source.SchemaType)
	}
	if envelope.EventID == "" {
		t.Errorf("EventID không được để chuỗi rỗng")
	}
}

// TestPlayerStatNormalizer_DeterministicEventID kiểm tra tính bất biến của SHA-256 event_id khi cùng input
// Schema: finish_placement
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
// Schema: finish_placement
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

// TestPlayerStatNormalizer_MatchDeaths_ValidRecord kiểm tra chuẩn hóa kill event từ kill_match_stats_final_*.csv
// Schema: match_deaths (killer_name, match_id, killed_by, killer_placement, killer_position_x, killer_position_y, time)
func TestPlayerStatNormalizer_MatchDeaths_ValidRecord(t *testing.T) {
	// Tạo normalizer mới — chưa cache schema
	normalizer := service.NewPlayerStatNormalizer("skihikingkevin-pubg-match-deaths")

	raw := &service.RawRecord{
		SourceFile:  "kill_match_stats_final_0.csv",
		RecordIndex: 1,
		Fields: map[string]string{
			"killed_by":          "SCAR-L",
			"killer_name":        "SniperPro99",
			"killer_placement":   "4.0",
			"killer_position_x":  "657725.1",
			"killer_position_y":  "146275.2",
			"map":                "MIRAMAR",
			"match_id":           "abc123matchXYZ",
			"time":               "823",
			"victim_name":        "VictimPlayer1",
			"victim_placement":   "5.0",
			"victim_position_x":  "657000.0",
			"victim_position_y":  "146000.0",
		},
	}

	envelope, invalid, err := normalizer.Normalize(raw)
	if err != nil {
		t.Fatalf("Normalize trả về lỗi bất ngờ: %v", err)
	}
	if invalid != nil {
		t.Fatalf("Kill event hợp lệ bị đánh dấu là Invalid: %+v", invalid.ValidationErrors)
	}
	if envelope == nil {
		t.Fatalf("EventEnvelope không được nil với kill event hợp lệ")
	}

	// Mỗi kill event = 1 kill
	if envelope.Payload.Kills != 1 {
		t.Errorf("Kill event phải có Kills=1, nhận được: %d", envelope.Payload.Kills)
	}
	// player_id = killer_name, match_id = match_id
	if envelope.PlayerID != "SniperPro99" {
		t.Errorf("PlayerID sai: %s, kỳ vọng: SniperPro99", envelope.PlayerID)
	}
	if envelope.MatchID != "abc123matchXYZ" {
		t.Errorf("MatchID sai: %s, kỳ vọng: abc123matchXYZ", envelope.MatchID)
	}
	// SurvivalDuration = "time" = 823
	if envelope.Payload.SurvivalDuration != 823.0 {
		t.Errorf("SurvivalDuration sai: %.2f, kỳ vọng: 823.0", envelope.Payload.SurvivalDuration)
	}
	// WinPlacePerc = 1/placement = 1/4 = 0.25
	if envelope.Payload.WinPlacePerc == nil {
		t.Errorf("WinPlacePerc không được nil khi killer_placement hợp lệ")
	} else if *envelope.Payload.WinPlacePerc != 0.25 {
		t.Errorf("WinPlacePerc sai: %.4f, kỳ vọng: 0.25", *envelope.Payload.WinPlacePerc)
	}
	// Verify Op discriminator — phải là OpKillEvent cho schema match_deaths
	if envelope.Op != service.ContractOpKillEvent {
		t.Errorf("Op sai: %s, kỳ vọng: %s", envelope.Op, service.ContractOpKillEvent)
	}
	// Verify SchemaType — phải tag đúng schema nguồn gốc
	if envelope.Source.SchemaType != "match_deaths" {
		t.Errorf("SchemaType sai: %s, kỳ vọng: match_deaths", envelope.Source.SchemaType)
	}
	if envelope.EventID == "" {
		t.Errorf("EventID không được để chuỗi rỗng")
	}
}

// TestPlayerStatNormalizer_MatchDeaths_MissingFields kiểm tra bắt lỗi khi thiếu killer_name hoặc match_id
// Schema: match_deaths
func TestPlayerStatNormalizer_MatchDeaths_MissingFields(t *testing.T) {
	normalizer := service.NewPlayerStatNormalizer("skihikingkevin-pubg-match-deaths")

	raw := &service.RawRecord{
		SourceFile:  "kill_match_stats_final_0.csv",
		RecordIndex: 99,
		Fields: map[string]string{
			// killer_name bị thiếu
			"killed_by":   "Grenade",
			"match_id":    "abc123matchXYZ",
			"victim_name": "VictimPlayer1",
			"time":        "100",
		},
	}

	envelope, invalid, err := normalizer.Normalize(raw)
	if err != nil {
		t.Fatalf("Normalize không được ném error với invalid record: %v", err)
	}
	if envelope != nil {
		t.Errorf("Envelope phải là nil khi bản ghi thiếu killer_name")
	}
	if invalid == nil {
		t.Fatalf("Kỳ vọng trả về InvalidRecord nhưng nhận được nil")
	}
}

// TestPlayerStatNormalizer_SchemaAutoDetect kiểm tra khả năng tự động phát hiện schema
func TestPlayerStatNormalizer_SchemaAutoDetect(t *testing.T) {
	t.Run("finish_placement schema", func(t *testing.T) {
		n := service.NewPlayerStatNormalizer("test")
		raw := &service.RawRecord{
			Fields: map[string]string{
				"Id":      "player-1",
				"matchId": "match-1",
				"kills":   "0",
			},
		}
		env, inv, _ := n.Normalize(raw)
		// Không có headshotKills/damage nên có thể nil hoặc valid, quan trọng là không panic
		if inv != nil && env != nil {
			t.Errorf("Không thể vừa có envelope vừa có invalid")
		}
	})

	t.Run("match_deaths schema", func(t *testing.T) {
		n := service.NewPlayerStatNormalizer("test")
		raw := &service.RawRecord{
			Fields: map[string]string{
				"killer_name": "player-1",
				"match_id":    "match-1",
				"victim_name": "victim-1",
				"time":        "100",
			},
		}
		env, inv, err := n.Normalize(raw)
		if err != nil {
			t.Fatalf("Unexpected error: %v", err)
		}
		if inv != nil {
			t.Fatalf("Kill event bị đánh dấu Invalid: %+v", inv.ValidationErrors)
		}
		if env == nil || env.Payload.Kills != 1 {
			t.Errorf("Schema match_deaths phải cho Kills=1 mỗi record")
		}
	})
}
