package app

import (
	"context"
	"encoding/json"
	"fmt"
	"strings"

	"github.com/sirupsen/logrus"

	"pubg-anti-cheat/go-ingestor/internal/config"
	"pubg-anti-cheat/go-ingestor/internal/dataset"
	"pubg-anti-cheat/go-ingestor/internal/normalize"
	"pubg-anti-cheat/go-ingestor/internal/parser"
	"pubg-anti-cheat/go-ingestor/internal/replay"
	"pubg-anti-cheat/go-ingestor/internal/storage"
)

// ReplayApp điều phối usecase đọc manifest từ MinIO, mở stream CSV và khởi chạy Replayer Loop
type ReplayApp struct {
	cfg      *config.Config       // Cấu hình ứng dụng
	minioCli *storage.MinIOClient // MinIO S3 Client
	log      *logrus.Entry        // Logger JSON
}

// NewReplayApp khởi tạo ReplayApp với các dependencies
func NewReplayApp(cfg *config.Config, log *logrus.Entry) (*ReplayApp, error) {
	minioCli, err := storage.NewMinIOClient(
		cfg.MinIOEndpoint,
		cfg.MinIOAccessKey,
		cfg.MinIOSecretKey,
		cfg.MinIOBucket,
		cfg.MinIOUseSSL,
	)
	if err != nil {
		return nil, fmt.Errorf("không thể kết nối MinIO Storage: %w", err)
	}

	return &ReplayApp{
		cfg:      cfg,
		minioCli: minioCli,
		log:      log,
	}, nil
}

// Run thực thi Use Case Replay Dataset
func (a *ReplayApp) Run(ctx context.Context, replayCfg replay.ReplayerConfig) (*replay.ReplayStatistics, error) {
	a.log.Info("Khởi động Use Case Replay Dataset...")

	// 1. Chuẩn bị key đọc manifest trên MinIO S3
	manifestObjectKey := "manifests/dataset-manifest.json"

	// 2. Đọc file dataset-manifest.json từ MinIO S3
	a.log.WithField("manifest_key", manifestObjectKey).Info("Đang đọc Dataset Manifest từ MinIO S3...")
	manifestObj, err := a.minioCli.DownloadStream(ctx, manifestObjectKey)
	if err != nil {
		return nil, fmt.Errorf("không thể tải Dataset Manifest từ MinIO: %w", err)
	}
	defer manifestObj.Close()

	var manifest dataset.DatasetManifest
	if err := json.NewDecoder(manifestObj).Decode(&manifest); err != nil {
		return nil, fmt.Errorf("không thể decode Dataset Manifest JSON: %w", err)
	}

	a.log.WithFields(logrus.Fields{
		"dataset_id":     manifest.DatasetID,
		"extracted_path": manifest.ExtractedPath,
		"selected_file":  manifest.SelectedFile,
	}).Info("Đã nạp thành công Dataset Manifest")

	// 3. Tải Stream file CSV tĩnh từ MinIO S3 (`raw-sources/...`)
	a.log.WithField("csv_key", manifest.ExtractedPath).Info("Mở luồng đọc Stream file CSV từ MinIO S3...")
	csvObj, err := a.minioCli.DownloadStream(ctx, manifest.ExtractedPath)
	if err != nil {
		return nil, fmt.Errorf("không thể tải CSV stream từ MinIO: %w", err)
	}
	defer csvObj.Close()

	// 4. Khởi tạo CSV Streaming Parser (O(1) RAM footprint)
	csvParser, err := parser.NewCSVParser(csvObj, manifest.SelectedFile)
	if err != nil {
		return nil, fmt.Errorf("khởi tạo CSVParser thất bại: %w", err)
	}
	defer csvParser.Close()

	// 5. Khởi tạo PlayerStatNormalizer
	normalizer := normalize.NewPlayerStatNormalizer(manifest.DatasetID)

	// 6. Khởi tạo và thực thi Replayer Loop Engine
	replayerEngine := replay.NewReplayer(replayCfg, csvParser, normalizer, a.log)
	stats, err := replayerEngine.Run(ctx)
	if err != nil && !strings.Contains(err.Error(), "context canceled") {
		return stats, fmt.Errorf("lỗi trong vòng lặp Replay Loop: %w", err)
	}

	a.log.WithFields(logrus.Fields{
		"records_read":     stats.RecordsRead,
		"valid_records":    stats.ValidRecords,
		"invalid_records":  stats.InvalidRecords,
		"produced_records": stats.ProducedRecords,
	}).Info("Báo cáo thống kê Replay Dataset hoàn tất xuất sắc!")

	return stats, nil
}
