package replay_test

import (
	"bytes"
	"context"
	"os"
	"testing"

	"github.com/sirupsen/logrus"

	"pubg-anti-cheat/go-ingestor/internal/normalize"
	"pubg-anti-cheat/go-ingestor/internal/parser"
	"pubg-anti-cheat/go-ingestor/internal/replay"
)

// TestReplayer_DryRunAndLimit kiểm tra Replay Loop với các tùy chọn DryRun, StartRecord và Limit
func TestReplayer_DryRunAndLimit(t *testing.T) {
	// Logger phục vụ test
	logger := logrus.NewEntry(logrus.New())

	// CSV Data mẫu 3 bản ghi
	csvData := `Id,groupId,matchId,kills,damageDealt,headshotKills,walkDistance,rideDistance,swimDistance,matchDuration,winPlacePerc
player-1,group-10,match-100,5,550.5,2,1200.0,0.0,0.0,900.0,0.85
player-2,group-10,match-100,2,210.0,0,850.5,0.0,0.0,900.0,0.85
player-3,group-11,match-100,0,45.0,0,300.0,0.0,0.0,400.0,0.20`

	buf := bytes.NewBufferString(csvData)
	p, err := parser.NewCSVParser(buf, "test.csv")
	if err != nil {
		t.Fatalf("Khởi tạo CSVParser thất bại: %v", err)
	}

	normalizer := normalize.NewPlayerStatNormalizer("test-dataset")

	// Replay Config: Giới hạn chỉ đọc 2 bản ghi
	cfg := replay.ReplayerConfig{
		Limit:       2,
		StartRecord: 1,
		DryRun:      true,
	}

	replayerEngine := replay.NewReplayer(cfg, p, normalizer, logger)
	stats, err := replayerEngine.Run(context.Background())
	if err != nil {
		t.Fatalf("Replayer.Run trả về lỗi bất ngờ: %v", err)
	}

	// Kiểm tra chỉ đếm đúng 2 bản ghi do Limit = 2
	if stats.RecordsRead != 2 {
		t.Errorf("Kỳ vọng RecordsRead = 2, nhận được = %d", stats.RecordsRead)
	}
	if stats.ValidRecords != 2 {
		t.Errorf("Kỳ vọng ValidRecords = 2, nhận được = %d", stats.ValidRecords)
	}
	if stats.ProducedRecords != 2 {
		t.Errorf("Kỳ vọng ProducedRecords = 2, nhận được = %d", stats.ProducedRecords)
	}
}

// TestReplayer_StartRecordOffset kiểm tra tính năng bỏ qua các bản ghi trước StartRecord
func TestReplayer_StartRecordOffset(t *testing.T) {
	logger := logrus.NewEntry(logrus.New())

	file, err := os.Open("../parser/testdata/valid.csv")
	if err != nil {
		t.Fatalf("Không thể mở file testdata: %v", err)
	}
	defer file.Close()

	p, err := parser.NewCSVParser(file, "train_V2.csv")
	if err != nil {
		t.Fatalf("Khởi tạo CSVParser thất bại: %v", err)
	}

	normalizer := normalize.NewPlayerStatNormalizer("test-dataset")

	// Replay Config: Bắt đầu từ bản ghi thứ 2 (bỏ qua bản ghi 1)
	cfg := replay.ReplayerConfig{
		Limit:       0,
		StartRecord: 2,
		DryRun:      true,
	}

	replayerEngine := replay.NewReplayer(cfg, p, normalizer, logger)
	stats, err := replayerEngine.Run(context.Background())
	if err != nil {
		t.Fatalf("Replayer.Run trả về lỗi bất ngờ: %v", err)
	}

	// Trong valid.csv có 3 bản ghi, bắt đầu từ 2 nên đọc được 2 bản ghi
	if stats.RecordsRead != 2 {
		t.Errorf("Kỳ vọng RecordsRead = 2 (khi StartRecord = 2), nhận được = %d", stats.RecordsRead)
	}
}

// TestReplayer_ContextCancellation kiểm tra ngắt an toàn khi Context bị Cancel
func TestReplayer_ContextCancellation(t *testing.T) {
	logger := logrus.NewEntry(logrus.New())

	csvData := `Id,groupId,matchId,kills,damageDealt,headshotKills,walkDistance,rideDistance,swimDistance,matchDuration,winPlacePerc
player-1,group-10,match-100,5,550.5,2,1200.0,0.0,0.0,900.0,0.85`

	buf := bytes.NewBufferString(csvData)
	p, _ := parser.NewCSVParser(buf, "test.csv")
	normalizer := normalize.NewPlayerStatNormalizer("test-dataset")

	ctx, cancel := context.WithCancel(context.Background())
	cancel() // Hủy context ngay lập tức

	cfg := replay.ReplayerConfig{DryRun: true}
	replayerEngine := replay.NewReplayer(cfg, p, normalizer, logger)

	_, err := replayerEngine.Run(ctx)
	if err == nil {
		t.Errorf("Kỳ vọng trả về lỗi context canceled nhưng nhận được nil")
	}
}
