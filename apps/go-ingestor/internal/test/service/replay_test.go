package service_test

import (
	"bytes"
	"context"
	"errors"
	"os"
	"testing"

	"github.com/sirupsen/logrus"

	"pubg-anti-cheat/go-ingestor/internal/service"
)

// TestReplayer_DryRunAndLimit kiểm tra Replay Loop với các tùy chọn DryRun, StartRecord và Limit
func TestReplayer_DryRunAndLimit(t *testing.T) {
	logger := logrus.NewEntry(logrus.New())

	csvData := `Id,groupId,matchId,kills,damageDealt,headshotKills,walkDistance,rideDistance,swimDistance,matchDuration,winPlacePerc
player-1,group-10,match-100,5,550.5,2,1200.0,0.0,0.0,900.0,0.85
player-2,group-10,match-100,2,210.0,0,850.5,0.0,0.0,900.0,0.85
player-3,group-11,match-100,0,45.0,0,300.0,0.0,0.0,400.0,0.20`

	buf := bytes.NewBufferString(csvData)
	p, err := service.NewCSVParser(buf, "test.csv")
	if err != nil {
		t.Fatalf("Khởi tạo CSVParser thất bại: %v", err)
	}

	normalizer := service.NewPlayerStatNormalizer("test-dataset")

	cfg := service.ReplayerConfig{
		Limit:       2,
		StartRecord: 1,
		DryRun:      true,
	}

	replayerEngine := service.NewReplayer(cfg, p, normalizer, nil, nil, "test-dataset", "test.csv", "sha256-test", logger)
	stats, err := replayerEngine.Run(context.Background())
	if err != nil {
		t.Fatalf("Replayer.Run trả về lỗi bất ngờ: %v", err)
	}

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

	file, err := os.Open("testdata/valid.csv")
	if err != nil {
		t.Fatalf("Không thể mở file testdata: %v", err)
	}
	defer file.Close()

	p, err := service.NewCSVParser(file, "train_V2.csv")
	if err != nil {
		t.Fatalf("Khởi tạo CSVParser thất bại: %v", err)
	}

	normalizer := service.NewPlayerStatNormalizer("test-dataset")

	cfg := service.ReplayerConfig{
		Limit:       0,
		StartRecord: 2,
		DryRun:      true,
	}

	replayerEngine := service.NewReplayer(cfg, p, normalizer, nil, nil, "test-dataset", "train_V2.csv", "sha256-test", logger)
	stats, err := replayerEngine.Run(context.Background())
	if err != nil {
		t.Fatalf("Replayer.Run trả về lỗi bất ngờ: %v", err)
	}

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
	p, _ := service.NewCSVParser(buf, "test.csv")
	normalizer := service.NewPlayerStatNormalizer("test-dataset")

	ctx, cancel := context.WithCancel(context.Background())
	cancel()

	cfg := service.ReplayerConfig{DryRun: true}
	replayerEngine := service.NewReplayer(cfg, p, normalizer, nil, nil, "test-dataset", "test.csv", "sha256-test", logger)

	stats, err := replayerEngine.Run(ctx)
	if err != nil && !errors.Is(err, context.Canceled) {
		t.Errorf("Lỗi không mong muốn khi context canceled: %v", err)
	}
	if stats == nil {
		t.Errorf("Stats không được nil")
	}
}
