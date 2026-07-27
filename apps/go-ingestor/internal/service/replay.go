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
	producer   Producer // Kafka Producer (Fail-Close)
	log        *logrus.Entry
	stats      ReplayStatistics
}

// NewReplayer khởi tạo Replayer với optional producer
func NewReplayer(cfg ReplayerConfig, p Parser, n Normalizer, producer Producer, log *logrus.Entry) *Replayer {
	return &Replayer{
		cfg:        cfg,
		parser:     p,
		normalizer: n,
		producer:   producer,
		log:        log,
		stats:      ReplayStatistics{},
	}
}

// Run thực thi vòng lặp Replay Loop với cơ chế Fail-Close khi Kafka phát lỗi
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

		// Xử lý bản ghi bị vi phạm Validation (Invalid Record)
		if invalidRecord != nil {
			r.stats.InvalidRecords++
			if !r.cfg.DryRun && r.producer != nil {
				// Phát bản ghi lỗi vào DLQ Topic; nếu thất bại -> Fail-Close ngắt tiến trình!
				if err := r.producer.ProduceInvalid(ctx, invalidRecord); err != nil {
					r.log.WithError(err).Error("Fail-Close: Không thể phát bản ghi lỗi vào Kafka DLQ")
					return &r.stats, fmt.Errorf("fail-close DLQ produce: %w", err)
				}
			}
			continue
		}

		// Xử lý bản ghi hợp lệ (Valid Event Envelope)
		if envelope != nil {
			r.stats.ValidRecords++

			if r.cfg.DryRun {
				r.stats.ProducedRecords++
				if r.stats.ProducedRecords%1000 == 0 || r.stats.ProducedRecords == 1 {
					r.log.WithFields(logrus.Fields{
						"event_id":  envelope.EventID,
						"match_id":  envelope.MatchID,
						"player_id": envelope.PlayerID,
					}).Info("[Dry-Run] Mẫu Event Envelope được chuẩn hóa thành công")
				}
			} else if r.producer != nil {
				// Phát bản ghi hợp lệ vào Kafka Raw Topic (Key = match_id); nếu thất bại -> Fail-Close!
				if err := r.producer.ProduceEvent(ctx, envelope); err != nil {
					r.log.WithError(err).Error("Fail-Close: Không thể phát bản ghi vào Kafka Raw Topic")
					return &r.stats, fmt.Errorf("fail-close raw produce: %w", err)
				}
				r.stats.ProducedRecords++
			}
		}
	}

	return &r.stats, nil
}

// ReplayService điều phối usecase Replay Dataset từ MinIO S3 phát vào Kafka
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
	s.log.Info("Khởi động Use Case Replay Dataset (Kafka Integrated)...")

	// 1. Nếu không chạy Dry-Run, khởi tạo Kafka Producer (Fail-Close nếu thiếu config)
	var kafkaProducer Producer
	if !replayCfg.DryRun {
		var err error
		kafkaProducer, err = NewKafkaProducer(s.cfg.KafkaBrokers, s.cfg.KafkaRawTopic, s.cfg.KafkaInvalidTopic, s.log)
		if err != nil {
			return nil, fmt.Errorf("khởi tạo Kafka Producer thất bại (Fail-Close): %w", err)
		}
		defer kafkaProducer.Close()
	}

	// 2. Đọc Dataset Manifest từ MinIO S3
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

	// 3. Tải luồng CSV stream từ MinIO S3
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

	// 4. Khởi tạo và thực thi Replayer Loop với Producer
	replayerEngine := NewReplayer(replayCfg, csvParser, normalizer, kafkaProducer, s.log)
	stats, err := replayerEngine.Run(ctx)
	if err != nil && !strings.Contains(err.Error(), "context canceled") {
		return stats, fmt.Errorf("lỗi trong vòng lặp Replay Loop (Fail-Close Triggered): %w", err)
	}

	return stats, nil
}
