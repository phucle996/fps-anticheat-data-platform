package service

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"strings"
	"time"

	"github.com/sirupsen/logrus"

	"pubg-anti-cheat/go-ingestor/internal/config"
)

// ReplayerConfig định nghĩa thông số điều khiển replay
type ReplayerConfig struct {
	Limit       int64 // Số lượng bản ghi tối đa cần replay (0 = không giới hạn)
	StartRecord int64 // Chỉ số bản ghi bắt đầu replay (1 = dòng đầu tiên)
	DryRun      bool  // Cờ chạy thử không phát Kafka
}

// ReplayStatistics theo dõi bộ đếm thống kê thời gian thực của replay loop
type ReplayStatistics struct {
	RecordsRead     int64         `json:"records_read"`
	ValidRecords    int64         `json:"valid_records"`
	InvalidRecords  int64         `json:"invalid_records"`
	ProducedRecords int64         `json:"produced_records"`
	Duration        time.Duration `json:"duration"`
}

// Replayer Engine vòng lặp Replay
type Replayer struct {
	cfg        ReplayerConfig
	parser     Parser
	normalizer Normalizer
	log        *logrus.Entry
	stats      ReplayStatistics
}

// NewReplayer khởi tạo Replayer
func NewReplayer(cfg ReplayerConfig, p Parser, n Normalizer, log *logrus.Entry) *Replayer {
	return &Replayer{
		cfg:        cfg,
		parser:     p,
		normalizer: n,
		log:        log,
		stats:      ReplayStatistics{},
	}
}

// Run thực thi vòng lặp Replay Loop
func (r *Replayer) Run(ctx context.Context) (*ReplayStatistics, error) {
	startTime := time.Now()
	r.log.WithFields(logrus.Fields{
		"start_record": r.cfg.StartRecord,
		"limit":        r.cfg.Limit,
		"dry_run":      r.cfg.DryRun,
	}).Info("Bắt đầu vòng lặp Replay Loop...")

	defer func() {
		r.stats.Duration = time.Since(startTime)
		r.log.WithFields(logrus.Fields{
			"records_read":     r.stats.RecordsRead,
			"valid_records":    r.stats.ValidRecords,
			"invalid_records":  r.stats.InvalidRecords,
			"produced_records": r.stats.ProducedRecords,
			"duration_ms":      r.stats.Duration.Milliseconds(),
		}).Info("Kết thúc vòng lặp Replay Loop.")
	}()

	for {
		select {
		case <-ctx.Done():
			r.log.Warn("Nhận tín hiệu ngắt Context, dừng vòng lặp Replay...")
			return &r.stats, ctx.Err()
		default:
		}

		if r.cfg.Limit > 0 && r.stats.RecordsRead >= r.cfg.Limit {
			r.log.WithField("limit", r.cfg.Limit).Info("Đã đạt giới hạn số bản ghi Limit, hoàn tất Replay.")
			break
		}

		rawRecord, err := r.parser.Next()
		if err != nil {
			if errors.Is(err, ErrEOF) {
				r.log.Info("Đã đọc tới cuối file CSV (EOF).")
				break
			}
			return &r.stats, fmt.Errorf("lỗi parser read: %w", err)
		}

		if r.cfg.StartRecord > 1 && rawRecord.RecordIndex < r.cfg.StartRecord {
			continue
		}

		r.stats.RecordsRead++

		envelope, invalidRecord, normErr := r.normalizer.Normalize(rawRecord)
		if normErr != nil {
			return &r.stats, normErr
		}

		if invalidRecord != nil {
			r.stats.InvalidRecords++
			continue
		}

		if envelope != nil {
			r.stats.ValidRecords++
			r.stats.ProducedRecords++
		}
	}

	return &r.stats, nil
}

// ReplayService điều phối usecase Replay Dataset từ MinIO S3
type ReplayService struct {
	cfg      *config.Config
	minioCli *MinIOClient
	log      *logrus.Entry
}

// NewReplayService khởi tạo ReplayService
func NewReplayService(cfg *config.Config, log *logrus.Entry) (*ReplayService, error) {
	minioCli, err := NewMinIOClient(
		cfg.MinIOEndpoint,
		cfg.MinIOAccessKey,
		cfg.MinIOSecretKey,
		cfg.MinIOBucket,
		cfg.MinIOUseSSL,
	)
	if err != nil {
		return nil, fmt.Errorf("không thể kết nối MinIO Storage: %w", err)
	}

	return &ReplayService{
		cfg:      cfg,
		minioCli: minioCli,
		log:      log,
	}, nil
}

// Run thực thi Replay Service
func (s *ReplayService) Run(ctx context.Context, replayCfg ReplayerConfig) (*ReplayStatistics, error) {
	s.log.Info("Khởi động Use Case Replay Dataset (Flat Architecture)...")

	manifestObjectKey := "manifests/dataset-manifest.json"
	manifestObj, err := s.minioCli.DownloadStream(ctx, manifestObjectKey)
	if err != nil {
		return nil, fmt.Errorf("không thể tải Dataset Manifest từ MinIO: %w", err)
	}
	defer manifestObj.Close()

	var manifest DatasetManifest
	if err := json.NewDecoder(manifestObj).Decode(&manifest); err != nil {
		return nil, fmt.Errorf("không thể decode Dataset Manifest JSON: %w", err)
	}

	csvObj, err := s.minioCli.DownloadStream(ctx, manifest.ExtractedPath)
	if err != nil {
		return nil, fmt.Errorf("không thể tải CSV stream từ MinIO: %w", err)
	}
	defer csvObj.Close()

	csvParser, err := NewCSVParser(csvObj, manifest.SelectedFile)
	if err != nil {
		return nil, fmt.Errorf("khởi tạo CSVParser thất bại: %w", err)
	}
	defer csvParser.Close()

	normalizer := NewPlayerStatNormalizer(manifest.DatasetID)

	replayerEngine := NewReplayer(replayCfg, csvParser, normalizer, s.log)
	stats, err := replayerEngine.Run(ctx)
	if err != nil && !strings.Contains(err.Error(), "context canceled") {
		return stats, fmt.Errorf("lỗi trong vòng lặp Replay Loop: %w", err)
	}

	return stats, nil
}
