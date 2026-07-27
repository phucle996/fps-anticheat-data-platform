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

// ReplayerConfig định nghĩa thông số điều khiển replay và checkpointing
type ReplayerConfig struct {
	Limit             int64       // Số lượng bản ghi tối đa cần replay (0 = không giới hạn)
	StartRecord       int64       // Chỉ số bản ghi bắt đầu replay (1 = dòng đầu tiên)
	DryRun            bool        // Cờ chạy thử không phát Kafka
	DisableCheckpoint bool        // Cờ tắt tính năng đọc/ghi Checkpoint
	ResetCheckpoint   bool        // Cờ xóa trạng thái Checkpoint cũ trên MinIO S3
	MicroBatching     BatchConfig // Cấu hình Micro-Batching Flusher
	StreamDelayMs     int64       // Khoảng trễ (ms) phát rải rác giữa các bản ghi game events (Stream Simulator)
}

// ReplayStatistics theo dõi bộ đếm thống kê thời gian thực của replay loop
type ReplayStatistics struct {
	RecordsRead     int64         `json:"records_read"`
	ValidRecords    int64         `json:"valid_records"`
	InvalidRecords  int64         `json:"invalid_records"`
	ProducedRecords int64         `json:"produced_records"`
	Duration        time.Duration `json:"duration"`
}

// Replayer Engine vòng lặp Replay tích hợp Micro-Batching Flusher & MinIO Checkpoint Store
type Replayer struct {
	cfg             ReplayerConfig
	parser          Parser
	normalizer      Normalizer
	producer        Producer        // Kafka Producer (Fail-Close)
	checkpointStore CheckpointStore // MinIO S3 Checkpoint Store
	flusher         *BatchFlusher   // Bộ đệm Micro-Batching
	datasetID       string          // ID dataset đang replay
	sourceFile      string          // Tên file CSV nguồn
	log             *logrus.Entry
	stats           ReplayStatistics
}

// NewReplayer khởi tạo Replayer với CheckpointStore
func NewReplayer(cfg ReplayerConfig, p Parser, n Normalizer, producer Producer, cpStore CheckpointStore, datasetID, sourceFile string, log *logrus.Entry) *Replayer {
	flusher := NewBatchFlusher(cfg.MicroBatching, producer)
	return &Replayer{
		cfg:             cfg,
		parser:          p,
		normalizer:      n,
		producer:        producer,
		checkpointStore: cpStore,
		flusher:         flusher,
		datasetID:       datasetID,
		sourceFile:      sourceFile,
		log:             log,
		stats:           ReplayStatistics{},
	}
}

// Run thực thi vòng lặp Replay Loop với cơ chế Resume từ Checkpoint và Fail-Close
func (r *Replayer) Run(ctx context.Context) (*ReplayStatistics, error) {
	startTime := time.Now()

	// 1. Xử lý tùy chọn Reset Checkpoint trên MinIO S3 nếu được yêu cầu
	if r.cfg.ResetCheckpoint && r.checkpointStore != nil {
		r.log.Info("Thực hiện Reset Checkpoint trạng thái cũ trên MinIO S3...")
		if err := r.checkpointStore.Reset(ctx); err != nil {
			r.log.WithError(err).Warn("Lỗi khi Reset Checkpoint trên MinIO S3")
		}
	}

	// 2. Tự động nạp Checkpoint từ MinIO S3 và Resume vị trí đọc nếu không bị Disable
	if !r.cfg.DisableCheckpoint && r.checkpointStore != nil && r.cfg.StartRecord <= 1 {
		cpState, err := r.checkpointStore.Load(ctx)
		if err != nil {
			r.log.WithError(err).Warn("Không thể nạp Checkpoint từ MinIO S3, chạy từ bản ghi mặc định")
		} else if cpState != nil && cpState.LastCompletedRecordIndex > 0 {
			// Resume từ chỉ số kế tiếp
			r.cfg.StartRecord = cpState.LastCompletedRecordIndex + 1
			r.log.WithFields(logrus.Fields{
				"last_completed": cpState.LastCompletedRecordIndex,
				"resume_start":   r.cfg.StartRecord,
			}).Info("Đã khôi phục thành công điểm dừng Replay (Resume) từ MinIO S3 Checkpoint!")
		}
	}

	r.log.WithFields(logrus.Fields{
		"start_record": r.cfg.StartRecord,
		"limit":        r.cfg.Limit,
		"dry_run":      r.cfg.DryRun,
		"batch_size":   r.cfg.MicroBatching.MaxBatchSize,
	}).Info("Bắt đầu vòng lặp Replay Loop (Checkpointing Active)...")

	// Kích hoạt Timer Flush nhịp định kỳ theo thời gian
	r.flusher.StartTimer(ctx)
	defer r.flusher.StopTimer()

	// Defer FlushAll và Lưu Checkpoint khi kết thúc (EOF hoặc Shutdown)
	defer func() {
		_ = r.flusher.FlushAll(context.Background())
		r.saveCheckpoint(context.Background())
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
			r.log.Warn("Nhận tín hiệu ngắt Context, dừng vòng lặp Replay và Flush bộ đệm...")
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
				r.log.Info("Đã đọc tới cuối file CSV (EOF), thực thi Flush bộ đệm cuối...")
				break
			}
			return &r.stats, fmt.Errorf("lỗi parser read: %w", err)
		}

		// Bỏ qua các bản ghi trước vị trí StartRecord (được nạp từ Checkpoint hoặc CLI flag)
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
			flushedCount, err := r.flusher.AddInvalid(ctx, invalidRecord)
			if err != nil {
				return &r.stats, fmt.Errorf("fail-close trong batch invalid flush: %w", err)
			}
			_ = flushedCount
			continue
		}

		if envelope != nil {
			r.stats.ValidRecords++

			flushedCount, err := r.flusher.AddEvent(ctx, envelope)
			if err != nil {
				return &r.stats, fmt.Errorf("fail-close trong batch raw flush: %w", err)
			}

			if r.cfg.DryRun {
				r.stats.ProducedRecords++
			} else {
				r.stats.ProducedRecords += flushedCount
				// Cập nhật và lưu Checkpoint lên MinIO S3 sau mỗi đợt phát Kafka thành công
				if flushedCount > 0 {
					r.saveCheckpoint(ctx)
				}
			}

			// Giả lập khoảng trễ phát rải rác thời gian thực (Real-time Stream Simulator)
			if r.cfg.StreamDelayMs > 0 {
				select {
				case <-ctx.Done():
					r.log.Warn("Ngắt Context trong lúc chờ StreamDelayMs")
					return &r.stats, ctx.Err()
				case <-time.After(time.Duration(r.cfg.StreamDelayMs) * time.Millisecond):
					// Hoàn tất khoảng trễ nạp bản ghi rải rác
				}
			}
		}
	}

	if err := r.flusher.FlushAll(ctx); err != nil {
		return &r.stats, fmt.Errorf("fail-close trong EOF flush final: %w", err)
	}
	r.saveCheckpoint(ctx)

	return &r.stats, nil
}

// saveCheckpoint lưu trạng thái record_index mới nhất lên MinIO S3
func (r *Replayer) saveCheckpoint(ctx context.Context) {
	if r.cfg.DisableCheckpoint || r.checkpointStore == nil || r.stats.RecordsRead == 0 {
		return
	}

	// Chỉ số bản ghi hoàn thành mới nhất = StartRecord + RecordsRead - 1
	lastIndex := r.cfg.StartRecord + r.stats.RecordsRead - 1
	if r.cfg.StartRecord <= 1 {
		lastIndex = r.stats.RecordsRead
	}

	state := &CheckpointState{
		DatasetID:                r.datasetID,
		SourceFile:               r.sourceFile,
		LastCompletedRecordIndex: lastIndex,
		UpdatedAt:                time.Now().UTC(),
	}

	if err := r.checkpointStore.Save(ctx, state); err != nil {
		r.log.WithError(err).Warn("Không thể cập nhật Checkpoint state lên MinIO S3")
	} else {
		r.log.WithField("last_completed", lastIndex).Debug("Đã lưu Checkpoint thành công lên MinIO S3")
	}
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

// Run thực thi Replay Service với MinIO S3 Checkpoint Store
func (s *ReplayService) Run(ctx context.Context, replayCfg ReplayerConfig) (*ReplayStatistics, error) {
	s.log.Info("Khởi động Use Case Replay Dataset (MinIO S3 Checkpoint Integrated)...")

	var kafkaProducer Producer
	if !replayCfg.DryRun {
		var err error
		kafkaProducer, err = NewKafkaProducer(s.cfg.KafkaBrokers, s.cfg.KafkaRawTopic, s.cfg.KafkaInvalidTopic, s.log)
		if err != nil {
			return nil, fmt.Errorf("khởi tạo Kafka Producer thất bại (Fail-Close): %w", err)
		}
		defer kafkaProducer.Close()
	}

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

	// Khởi tạo MinIOCheckpointStore trên pubg-data/checkpoints/go-replay/state.json
	cpStore := NewMinIOCheckpointStore(s.minioCli, "checkpoints/go-replay/state.json")

	replayerEngine := NewReplayer(replayCfg, csvParser, normalizer, kafkaProducer, cpStore, manifest.DatasetID, manifest.SelectedFile, s.log)
	stats, err := replayerEngine.Run(ctx)
	if err != nil && !strings.Contains(err.Error(), "context canceled") {
		return stats, fmt.Errorf("lỗi trong vòng lặp Replay Loop (Fail-Close Triggered): %w", err)
	}

	return stats, nil
}
