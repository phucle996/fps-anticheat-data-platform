package replay

import (
	"context"
	"errors"
	"fmt"
	"time"

	"github.com/sirupsen/logrus"

	"pubg-anti-cheat/go-ingestor/internal/contract"
	"pubg-anti-cheat/go-ingestor/internal/normalize"
	"pubg-anti-cheat/go-ingestor/internal/parser"
)

// ReplayerConfig định nghĩa các thông số điều khiển vòng lặp Replay
type ReplayerConfig struct {
	Limit       int64 // Số lượng bản ghi tối đa cần replay (0 = không giới hạn)
	StartRecord int64 // Chỉ số bản ghi bắt đầu replay (1 = dòng đầu tiên)
	DryRun      bool  // Cờ chạy thử không gửi tin nhắn vào Kafka
}

// ReplayStatistics theo dõi bộ đếm số liệu thống kê thời gian thực của vòng lặp Replay
type ReplayStatistics struct {
	RecordsRead     int64         `json:"records_read"`     // Tổng số bản ghi đã đọc từ CSV
	ValidRecords    int64         `json:"valid_records"`    // Số bản ghi hợp lệ
	InvalidRecords  int64         `json:"invalid_records"`  // Số bản ghi bị vi phạm validation / lỗi
	ProducedRecords int64         `json:"produced_records"` // Số bản ghi đã phát thành công vào Kafka (hoặc dry-run)
	Duration        time.Duration `json:"duration"`         // Tổng thời gian thực thi replay
}

// Replayer điều khiển vòng lặp đọc, chuẩn hóa và phát dữ liệu
type Replayer struct {
	cfg        ReplayerConfig             // Cấu hình replay
	parser     parser.Parser              // CSV Stream Parser
	normalizer normalize.Normalizer       // PlayerStat Normalizer
	log        *logrus.Entry              // Logger JSON
	stats      ReplayStatistics           // Thống kê tiến trình
}

// NewReplayer khởi tạo đối tượng Replayer
func NewReplayer(cfg ReplayerConfig, p parser.Parser, n normalize.Normalizer, log *logrus.Entry) *Replayer {
	return &Replayer{
		cfg:        cfg,
		parser:     p,
		normalizer: n,
		log:        log,
		stats:      ReplayStatistics{},
	}
}

// Run thực thi vòng lặp Replay Loop đến khi hết file, đạt Limit hoặc bị Hủy bởi Context
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
		// 1. Kiểm tra nếu Context bị Hủy / Graceful Shutdown từ hệ thống
		select {
		case <-ctx.Done():
			r.log.Warn("Nhận tín hiệu ngắt Context, dừng vòng lặp Replay...")
			return &r.stats, ctx.Err()
		default:
		}

		// 2. Kiểm tra điều kiện ngắt nếu đã đọc đủ số bản ghi quy định trong Limit
		if r.cfg.Limit > 0 && r.stats.RecordsRead >= r.cfg.Limit {
			r.log.WithField("limit", r.cfg.Limit).Info("Đã đạt giới hạn số bản ghi Limit, hoàn tất Replay.")
			break
		}

		// 3. Đọc bản ghi thô tiếp theo từ Parser Stream
		rawRecord, err := r.parser.Next()
		if err != nil {
			if errors.Is(err, parser.ErrEOF) {
				r.log.Info("Đã đọc tới cuối file CSV (EOF).")
				break
			}
			r.log.WithError(err).Error("Lỗi khi đọc bản ghi từ CSV Stream")
			return &r.stats, fmt.Errorf("lỗi parser read: %w", err)
		}

		// Bỏ qua các bản ghi trước vị trí StartRecord
		if r.cfg.StartRecord > 1 && rawRecord.RecordIndex < r.cfg.StartRecord {
			continue
		}

		// Cập nhật bộ đếm bản ghi đã đọc
		r.stats.RecordsRead++

		// 4. Gọi Normalizer để chuẩn hóa và validate bản ghi
		envelope, invalidRecord, normErr := r.normalizer.Normalize(rawRecord)
		if normErr != nil {
			r.log.WithError(normErr).Error("Lỗi hệ thống khi chuẩn hóa bản ghi")
			return &r.stats, normErr
		}

		// 5. Phân loại kết quả chuẩn hóa
		if invalidRecord != nil {
			r.stats.InvalidRecords++
			if r.cfg.DryRun {
				r.log.WithFields(logrus.Fields{
					"record_index": rawRecord.RecordIndex,
					"errors":       invalidRecord.ValidationErrors,
				}).Warn("[Dry-Run] Phát hiện bản ghi Invalid Record")
			}
			continue
		}

		if envelope != nil {
			r.stats.ValidRecords++

			// 6. Xử lý chế độ Dry-Run hoặc phát tin nhắn vào Kafka
			if r.cfg.DryRun {
				r.stats.ProducedRecords++
				if r.stats.ProducedRecords%1000 == 0 || r.stats.ProducedRecords == 1 {
					r.log.WithFields(logrus.Fields{
						"event_id":     envelope.EventID,
						"match_id":     envelope.MatchID,
						"player_id":    envelope.PlayerID,
						"record_index": envelope.Source.RecordIndex,
					}).Info("[Dry-Run] Mẫu Event Envelope được chuẩn hóa thành công")
				}
			} else {
				// Lưu ý: Ở Phase 8 sẽ tích hợp Producer phát envelope vào Kafka
				r.stats.ProducedRecords++
			}
		}
	}

	return &r.stats, nil
}

// GetStats trả về báo cáo thống kê hiện tại
func (r *Replayer) GetStats() ReplayStatistics {
	return r.stats
}

// Ensure interface contract check
var _ normalize.Normalizer = (normalize.Normalizer)(nil)
var _ contract.EventEnvelope = contract.EventEnvelope{}
