package service

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"runtime"
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
	flusher.SetLogger(log)
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

	// 0. Kiểm tra xem Context đã bị Hủy/Cancel trước khi bắt đầu hay chưa (Fail-Close Guard)
	if err := ctx.Err(); err != nil {
		r.log.Warn("Context đã bị hủy trước khi bắt đầu Replay Loop")
		return &r.stats, err
	}

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

	// Defer FlushAll cuối cùng và Lưu Checkpoint khi kết thúc (EOF hoặc Shutdown SIGINT/Ctrl+C)
	// Lưu ý: saveCheckpoint trong defer là nơi DUY NHẤT được gọi — tránh gọi trùng ngoài defer
	defer func() {
		// Flush nốt bộ đệm cuối cùng (Ctrl+C hoặc EOF) và cộng count vào ProducedRecords
		// DryRun: không flush Kafka thực tế nên không cộng count tránh đếm double
		if !r.cfg.DryRun {
			if finalCount, flushErr := r.flusher.FlushRaw(context.Background()); flushErr == nil && finalCount > 0 {
				r.stats.ProducedRecords += finalCount
			}
		}
		// Lưu Checkpoint cuối cùng lên MinIO S3 (bao gồm cả khi bị ngắt Ctrl+C)
		r.saveCheckpoint(context.Background())
		r.stats.Duration = time.Since(startTime)

		if ctx.Err() != nil {
			r.log.WithFields(logrus.Fields{
				"records_read":     r.stats.RecordsRead,
				"valid_records":    r.stats.ValidRecords,
				"invalid_records":  r.stats.InvalidRecords,
				"produced_records": r.stats.ProducedRecords,
				"duration_ms":      r.stats.Duration.Milliseconds(),
			}).Warn("⚠️ Tiến trình Replay đã dừng an toàn theo yêu cầu ngắt từ người dùng (Graceful Shutdown - SIGINT/Ctrl+C).")
		} else {
			r.log.WithFields(logrus.Fields{
				"records_read":     r.stats.RecordsRead,
				"valid_records":    r.stats.ValidRecords,
				"invalid_records":  r.stats.InvalidRecords,
				"produced_records": r.stats.ProducedRecords,
				"duration_ms":      r.stats.Duration.Milliseconds(),
			}).Info("Kết thúc vòng lặp Replay Loop.")
		}
	}()

	// Khởi tạo và khởi chạy Multi-Goroutine Worker Pool (Song song đa luồng CPU)
	workerPool := NewIngestionWorkerPool(runtime.NumCPU()*2, r.normalizer)
	workerPool.Start(ctx)

	// Goroutine Producer đọc file CSV và đẩy jobs vào Worker Pool
	go func() {
		defer workerPool.CloseJobs()

		var readCounter int64 = 0
		for {
			select {
			case <-ctx.Done():
				return
			default:
			}

			if r.cfg.Limit > 0 && readCounter >= r.cfg.Limit {
				r.log.WithField("limit", r.cfg.Limit).Info("Đã đạt giới hạn số bản ghi Limit, hoàn tất Replay Jobs Producer.")
				break
			}

			rawRecord, err := r.parser.Next()
			if err != nil {
				if errors.Is(err, ErrEOF) {
					r.log.Info("Đã đọc tới cuối file CSV (EOF), hoàn tất đẩy dữ liệu vào Worker Pool.")
					break
				}
				if ctx.Err() != nil {
					break
				}
				r.log.WithError(err).Error("Lỗi khi đọc bản ghi từ CSV Parser")
				break
			}

			// Bỏ qua các bản ghi trước vị trí StartRecord (Resume vị trí Checkpoint)
			if r.cfg.StartRecord > 1 && rawRecord.RecordIndex < r.cfg.StartRecord {
				continue
			}

			readCounter++
			workerPool.Submit(rawRecord)
		}
	}()

	// Main Goroutine tiêu thụ kết quả đã được các Goroutines normalize từ Worker Pool
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
			_ = flushedCount
			continue
		}

		if result.Envelope != nil {
			r.stats.ValidRecords++

			flushedCount, err := r.flusher.AddEvent(ctx, result.Envelope)
			if err != nil {
				return &r.stats, fmt.Errorf("fail-close trong batch raw flush: %w", err)
			}

			if r.cfg.DryRun {
				r.stats.ProducedRecords++
			} else {
				r.stats.ProducedRecords += flushedCount
				// Cập nhật Checkpoint lên MinIO S3 sau mỗi đợt phát Nano-batch Kafka thành công
				if flushedCount > 0 {
					r.saveCheckpoint(ctx)
				}
			}

			// Giả lập khoảng trễ phát rải rác thời gian thực nếu có cài đặt StreamDelayMs
			if r.cfg.StreamDelayMs > 0 {
				select {
				case <-ctx.Done():
					return &r.stats, ctx.Err()
				case <-time.After(time.Duration(r.cfg.StreamDelayMs) * time.Millisecond):
				}
			}
		}
	}

	// Đọc thống kê atomic từ Worker Pool
	r.stats.RecordsRead, _, _ = workerPool.Stats()

	// Kiểm tra nếu Context đã bị Hủy/Cancel trong quá trình xử lý
	// Khi người dùng ngắt Ctrl+C, đây là hành vi bình thường — trả về nil không phải error
	if ctx.Err() != nil {
		r.log.Warn("Nhận tín hiệu ngắt Context trong quá trình Replay Loop (Graceful Shutdown)")
		return &r.stats, nil
	}

	// EOF: Không gọi FlushAll hay saveCheckpoint ở đây nữa
	// defer ở trên đã xử lý toàn bộ FlushRaw cuối cùng và lưu Checkpoint đơn nhất
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
		// Log Info thay vì Debug để người dùng thấy rõ vị trí dừng trên Terminal
		r.log.WithFields(logrus.Fields{
			"last_completed": lastIndex,
			"dataset_id":    r.datasetID,
			"source_file":   r.sourceFile,
		}).Info("💾 [CHECKPOINT] Đã lưu vị trí Checkpoint lên MinIO S3. Có thể Resume từ dòng này sau khi Restart!")
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

	// Đảm bảo MinIO bucket chính tồn tại trước khi làm việc với S3
	if err := s.minioCli.EnsureBucketExists(ctx); err != nil {
		s.log.WithError(err).Warn("Không thể kiểm tra MinIO bucket, tiếp tục tiến trình...")
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

	// Kiểm tra sự tồn tại của file Dataset Manifest trên MinIO S3 bằng StatObject
	manifestExists, _ := s.minioCli.ObjectExists(ctx, manifestObjectKey)
	if !manifestExists {
		s.log.Warn("Chưa tìm thấy Dataset Manifest trên MinIO S3, tiến hành tự động khởi tạo Seed Dataset...")
		
		// Nội dung dataset CSV mẫu chuẩn PUBG cho môi trường Replay
		seedCSVHeader := "Id,groupId,matchId,kills,damageDealt,headshotKills,walkDistance,rideDistance,swimDistance,matchDuration,winPlacePerc\n"
		seedCSVRows := ""
		for i := 1; i <= 500; i++ {
			if i%10 == 0 {
				// Sinh bản ghi gian lận (Aimbot / High Headshot) phục vụ kiểm thử AI
				seedCSVRows += fmt.Sprintf("player_suspect_%03d,group_%02d,match_%03d,18,1450.0,15,250.0,0.0,0.0,600.0,0.99\n", i, i%5+1, i%10+1)
			} else {
				// Sinh bản ghi thi đấu hợp lệ chuẩn
				seedCSVRows += fmt.Sprintf("player_alpha_%03d,group_%02d,match_%03d,%d,%.1f,%d,%.1f,0.0,0.0,900.0,%.2f\n",
					i, i%5+1, i%10+1, i%5, float64(i%5)*120.0, (i%5)/3, float64(i%5)*300.0+100.0, 0.50)
			}
		}
		fullCSV := seedCSVHeader + seedCSVRows

		extractedKey := "raw-sources/pubg-dataset/train_V2.csv"
		_ = s.minioCli.UploadStream(ctx, extractedKey, strings.NewReader(fullCSV), int64(len(fullCSV)), "text/csv")

		manifest = DatasetManifest{
			DatasetID:       "kaggle-pubg-finish-placement-prediction",
			DatasetSlug:     s.cfg.DatasetSlug,
			ArchivePath:     "archives/pubg-dataset/dataset.zip",
			ArchiveChecksum: "seed-checksum",
			ExtractedPath:   extractedKey,
			SelectedFile:    s.cfg.SelectedFile,
			DownloadedAt:    time.Now().UTC(),
		}

		manifestBytes, _ := json.MarshalIndent(manifest, "", "  ")
		_ = s.minioCli.UploadStream(ctx, manifestObjectKey, bytes.NewReader(manifestBytes), int64(len(manifestBytes)), "application/json")
	} else {
		manifestObj, err := s.minioCli.DownloadStream(ctx, manifestObjectKey)
		if err != nil {
			return nil, fmt.Errorf("không thể tải Dataset Manifest từ MinIO: %w", err)
		}
		defer manifestObj.Close()

		if err := json.NewDecoder(manifestObj).Decode(&manifest); err != nil {
			return nil, fmt.Errorf("không thể decode Dataset Manifest JSON: %w", err)
		}
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
