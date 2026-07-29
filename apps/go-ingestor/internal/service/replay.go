package service

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"runtime"
	"strings"
	"time"

	"github.com/sirupsen/logrus"

	"pubg-anti-cheat/go-ingestor/internal/config"
	"pubg-anti-cheat/go-ingestor/internal/contract"
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

// Replayer Engine vòng lặp Replay tích hợp Micro-Batching Flusher & Contiguous ACK Checkpoint Store
type Replayer struct {
	cfg             ReplayerConfig
	parser          Parser
	normalizer      Normalizer
	producer        Producer              // Kafka Producer (Fail-Close)
	checkpointStore CheckpointStore       // MinIO S3 Checkpoint Store
	flusher         *BatchFlusher         // Bộ đệm Micro-Batching
	ackTracker      *ContiguousAckTracker // Tracker theo dõi highest contiguous ACKed index
	datasetID       string                // ID dataset đang replay
	datasetSHA256   string                // Hash SHA256 của dataset zip
	sourceFile      string                // Tên file CSV nguồn
	log             *logrus.Entry
	stats           ReplayStatistics
}

// NewReplayer khởi tạo Replayer với ContiguousAckTracker
func NewReplayer(
	cfg ReplayerConfig,
	p Parser,
	n Normalizer,
	producer Producer,
	cpStore CheckpointStore,
	datasetID, sourceFile, datasetSHA256 string,
	log *logrus.Entry,
) *Replayer {
	flusher := NewBatchFlusher(cfg.MicroBatching, producer)
	flusher.SetLogger(log)

	initialAckIndex := cfg.StartRecord - 1
	if initialAckIndex < 0 {
		initialAckIndex = 0
	}

	return &Replayer{
		cfg:             cfg,
		parser:          p,
		normalizer:      n,
		producer:        producer,
		checkpointStore: cpStore,
		flusher:         flusher,
		ackTracker:      NewContiguousAckTracker(initialAckIndex),
		datasetID:       datasetID,
		datasetSHA256:   datasetSHA256,
		sourceFile:      sourceFile,
		log:             log,
	}
}

// Run thực thi vòng lặp Replay phát tin nhắn bất đồng bộ qua Worker Pool
func (r *Replayer) Run(ctx context.Context) (*ReplayStatistics, error) {
	startTime := time.Now()
	defer func() {
		r.stats.Duration = time.Since(startTime)
	}()

	// Gán error handler cho Flusher để Fail-Fast ngắt loop nếu timer flush fail
	ctx, cancel := context.WithCancel(ctx)
	defer cancel()

	r.flusher.SetErrorHandler(func(err error) {
		r.log.WithError(err).Error("❌ [FAIL-FAST] Flusher gặp sự cố, ngắt Replayer Context!")
		cancel()
	})

	r.flusher.StartTimer(ctx)
	defer r.flusher.StopTimer()

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
		} else if cpState != nil && cpState.LastAckedRecordIndex > 0 {
			r.cfg.StartRecord = cpState.LastAckedRecordIndex + 1
			r.ackTracker = NewContiguousAckTracker(cpState.LastAckedRecordIndex)
			r.log.WithFields(logrus.Fields{
				"last_acked":   cpState.LastAckedRecordIndex,
				"resume_start": r.cfg.StartRecord,
			}).Info("💾 [CHECKPOINT RESUME] Đã khôi phục điểm dừng Replay liên tục từ MinIO S3!")
		}
	}

	r.log.WithFields(logrus.Fields{
		"start_record": r.cfg.StartRecord,
		"limit":        r.cfg.Limit,
		"dry_run":      r.cfg.DryRun,
		"batch_size":   r.cfg.MicroBatching.MaxBatchSize,
	}).Info("Bắt đầu vòng lặp Replay Loop (Contiguous ACK Checkpointing Active)...")

	// Khởi tạo IngestionWorkerPool
	numCPU := runtime.NumCPU() * 2
	workerPool := NewIngestionWorkerPool(numCPU, r.normalizer)
	workerPool.Start(ctx)

	// Background Goroutine đọc CSV từ Parser và đẩy vào Worker Pool
	go func() {
		defer workerPool.CloseJobs()
		var readCounter int64 = 0

		for {
			if r.cfg.Limit > 0 && readCounter >= r.cfg.Limit {
				r.log.WithField("limit", r.cfg.Limit).Info("Đã đạt giới hạn bản ghi replay (Limit reached).")
				break
			}

			rawRecord, err := r.parser.Next()
			if err != nil {
				if errors.Is(err, ErrEOF) {
					r.log.Info("Đã đọc tới cuối file CSV (EOF), hoàn tất đọc bản ghi.")
					break
				}
				if ctx.Err() != nil {
					break
				}
				r.log.WithError(err).Error("Lỗi khi đọc bản ghi từ CSV Parser")
				break
			}

			if r.cfg.StartRecord > 1 && rawRecord.RecordIndex < r.cfg.StartRecord {
				continue
			}

			readCounter++
			workerPool.Submit(rawRecord)
		}
	}()

	// Main Goroutine tiêu thụ kết quả từ Worker Pool
	for result := range workerPool.Results() {
		if ctx.Err() != nil {
			return &r.stats, nil
		}

		if result.Err != nil {
			return &r.stats, result.Err
		}

		if result.InvalidRecord != nil {
			r.stats.InvalidRecords++
			flushedCount, err := r.flusher.AddInvalid(ctx, result.InvalidRecord)
			if err != nil {
				return &r.stats, fmt.Errorf("fail-close trong batch invalid flush: %w", err)
			}
			// Ghi nhận ACK cho invalid record
			r.ackTracker.RecordAck(result.InvalidRecord.RecordIndex)
			_ = flushedCount
			continue
		}

		if result.Envelope != nil {
			r.stats.ValidRecords++

			flushedCount, err := r.flusher.AddEvent(ctx, result.Envelope)
			if err != nil {
				return &r.stats, fmt.Errorf("fail-close trong batch raw flush: %w", err)
			}

			// Lấy record_index từ envelope
			recIdx := extractRecordIndex(result.Envelope)
			r.ackTracker.RecordAck(recIdx)

			if r.cfg.DryRun {
				r.stats.ProducedRecords++
			} else {
				r.stats.ProducedRecords += flushedCount
				if flushedCount > 0 {
					r.saveCheckpoint(ctx)
				}
			}

			if r.cfg.StreamDelayMs > 0 {
				select {
				case <-ctx.Done():
					return &r.stats, ctx.Err()
				case <-time.After(time.Duration(r.cfg.StreamDelayMs) * time.Millisecond):
				}
			}
		}
	}

	r.stats.RecordsRead, _, _ = workerPool.Stats()

	if ctx.Err() != nil {
		r.log.Warn("Nhận tín hiệu ngắt Context trong quá trình Replay Loop (Graceful Shutdown)")
		return &r.stats, nil
	}

	// EOF: Flush sạch cả Raw lẫn Invalid bộ đệm TRƯỚC khi lưu Checkpoint cuối cùng
	r.log.Info("🏁 [EOF] Hoàn tất Replay file. Đang flush toàn bộ bộ đệm tồn đọng...")
	if err := r.flusher.FlushAll(ctx); err != nil {
		return &r.stats, fmt.Errorf("fail-close trong final flush tại EOF: %w", err)
	}

	// Lưu Checkpoint cuối cùng dựa trên Contiguous ACK high-water mark
	r.saveCheckpoint(ctx)

	return &r.stats, nil
}

// saveCheckpoint lưu trạng thái record_index liên tục cao nhất lên MinIO S3
func (r *Replayer) saveCheckpoint(ctx context.Context) {
	if r.cfg.DisableCheckpoint || r.checkpointStore == nil || r.stats.RecordsRead == 0 {
		return
	}

	lastAckedIndex := r.ackTracker.GetLastContiguousAcked()

	state := &CheckpointState{
		DatasetID:            r.datasetID,
		DatasetSHA256:        r.datasetSHA256,
		SourceFile:           r.sourceFile,
		SchemaVersion:        "kill-event-v1",
		LastAckedRecordIndex: lastAckedIndex,
		UpdatedAt:            time.Now().UTC(),
	}

	if err := r.checkpointStore.Save(ctx, state); err != nil {
		r.log.WithError(err).Warn("Không thể cập nhật Checkpoint state lên MinIO S3")
	} else {
		r.log.WithFields(logrus.Fields{
			"last_acked_contiguous": lastAckedIndex,
			"dataset_id":            r.datasetID,
			"source_file":           r.sourceFile,
		}).Info("💾 [CHECKPOINT] Đã lưu Contiguous ACK Checkpoint lên MinIO S3.")
	}
}

// Helper extractRecordIndex lấy record_index từ envelope
func extractRecordIndex(item interface{}) int64 {
	switch env := item.(type) {
	case *contract.KillEventEnvelope:
		return env.Source.RecordIndex
	case *contract.EventEnvelope:
		return env.Source.RecordIndex
	default:
		return 0
	}
}

// ReplayService điều phối usecase Replay Dataset từ MinIO S3 phát vào Kafka
type ReplayService struct {
	cfg      *config.Config
	minioCli *MinIOClient
	log      *logrus.Entry
}

func NewReplayService(cfg *config.Config, minioCli *MinIOClient, log *logrus.Entry) *ReplayService {
	return &ReplayService{
		cfg:      cfg,
		minioCli: minioCli,
		log:      log,
	}
}

func (s *ReplayService) RunReplay(ctx context.Context, replayCfg ReplayerConfig) (*ReplayStatistics, error) {
	s.log.WithFields(logrus.Fields{
		"dataset_slug":  s.cfg.DatasetSlug,
		"selected_file": s.cfg.SelectedFile,
		"brokers":       s.cfg.KafkaBrokers,
		"raw_topic":     s.cfg.KafkaRawTopic,
	}).Info("Bắt đầu Replay Dataset Use Case...")

	if err := s.minioCli.EnsureBucketExists(ctx); err != nil {
		return nil, fmt.Errorf("MinIO bucket '%s' không tồn tại hoặc lỗi kết nối: %w", s.cfg.MinIOBucket, err)
	}

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
	var manifest DatasetManifest

	manifestExists, _ := s.minioCli.ObjectExists(ctx, manifestObjectKey)
	if !manifestExists {
		return nil, fmt.Errorf("[FAIL-CLOSE] Bắt buộc chạy 'make sync' hoặc 'dataset-sync' trước để tải dataset Kaggle thật! Không tìm thấy manifest tại s3://%s/%s", s.cfg.MinIOBucket, manifestObjectKey)
	}

	stream, err := s.minioCli.DownloadStream(ctx, manifestObjectKey)
	if err != nil {
		return nil, fmt.Errorf("tải manifest thất bại: %w", err)
	}
	if err := json.NewDecoder(stream).Decode(&manifest); err != nil {
		stream.Close()
		return nil, fmt.Errorf("decode manifest JSON thất bại: %w", err)
	}
	stream.Close()

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
	cpStore := NewMinIOCheckpointStore(s.minioCli, manifest.ArchiveChecksum, manifest.SelectedFile)

	replayerEngine := NewReplayer(replayCfg, csvParser, normalizer, kafkaProducer, cpStore, manifest.DatasetID, manifest.SelectedFile, manifest.ArchiveChecksum, s.log)
	stats, err := replayerEngine.Run(ctx)
	if err != nil && !strings.Contains(err.Error(), "context canceled") {
		return stats, fmt.Errorf("lỗi trong vòng lặp Replay Loop (Fail-Close Triggered): %w", err)
	}

	return stats, nil
}
