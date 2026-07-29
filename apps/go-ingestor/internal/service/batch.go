package service

import (
	"context"
	"fmt"
	"sync"
	"sync/atomic"
	"time"

	"github.com/sirupsen/logrus"
	"pubg-anti-cheat/go-ingestor/internal/contract"
)

// BatchConfig định nghĩa cấu hình Micro-Batching
type BatchConfig struct {
	MaxBatchSize  int           // Số bản ghi tối đa trong micro-batch (Mặc định: 500 tin nhắn)
	MaxBatchBytes int64         // Kích thước byte tối đa trong micro-batch (Mặc định: 64KB)
	FlushInterval time.Duration // Nhịp timer flush (Mặc định: 500ms)
}

// BatchFlusher quản lý bộ đệm Micro-Batching với cơ chế Transactional Buffer chống mất dữ liệu
type BatchFlusher struct {
	cfg           BatchConfig
	producer      Producer
	rawEnvelopes  []interface{}             // Bộ đệm chứa *contract.EventEnvelope hoặc *contract.KillEventEnvelope
	rawBytes      int64                     // Dung lượng byte tích lũy của rawBuffer
	invalidRecs   []*contract.InvalidRecord // Bộ đệm mảng sự kiện vi phạm
	invalidBytes  int64                     // Dung lượng byte tích lũy của invalidBuffer
	mu            sync.Mutex                // Mutex bảo vệ truy cập đồng thời
	timerTicker   *time.Ticker              // Timer ticker nhịp flush định kỳ
	stopTickerCh  chan struct{}             // Channel báo dừng ticker
	stopOnce      sync.Once                 // Guard đảm bảo channel chỉ close 1 lần
	log           *logrus.Entry             // Logger
	batchCounter  atomic.Int64              // Bộ đếm tổng số batch đã flush
	totalProduced atomic.Int64              // Bộ đếm tổng số bản ghi đã phát sang Kafka
	onFlushErr    func(err error)           // Error handler callback khi timer flush thất bại (Fail-Fast)
}

// NewBatchFlusher khởi tạo BatchFlusher với Transactional Buffer
func NewBatchFlusher(cfg BatchConfig, producer Producer) *BatchFlusher {
	if cfg.MaxBatchSize <= 0 {
		cfg.MaxBatchSize = 500
	}
	if cfg.MaxBatchBytes <= 0 {
		cfg.MaxBatchBytes = 65536
	}
	if cfg.FlushInterval <= 0 {
		cfg.FlushInterval = 500 * time.Millisecond
	}

	return &BatchFlusher{
		cfg:          cfg,
		producer:     producer,
		rawEnvelopes: make([]interface{}, 0, cfg.MaxBatchSize),
		rawBytes:     0,
		invalidRecs:  make([]*contract.InvalidRecord, 0, cfg.MaxBatchSize),
		invalidBytes: 0,
		stopTickerCh: make(chan struct{}),
	}
}

// SetLogger gán logger cho BatchFlusher
func (b *BatchFlusher) SetLogger(log *logrus.Entry) {
	b.log = log
}

// SetErrorHandler gán error callback (ví dụ cancel context để Fail-Fast)
func (b *BatchFlusher) SetErrorHandler(fn func(err error)) {
	b.onFlushErr = fn
}

// StartTimer kích hoạt vòng lặp Flush định kỳ 500ms (Fail-Fast: KHÔNG nuốt lỗi)
func (b *BatchFlusher) StartTimer(ctx context.Context) {
	b.timerTicker = time.NewTicker(b.cfg.FlushInterval)
	go func() {
		for {
			select {
			case <-b.timerTicker.C:
				if err := b.FlushAll(ctx); err != nil {
					if b.log != nil {
						b.log.WithError(err).Error("❌ [FAIL-FAST] Periodic flush thất bại! Kích hoạt shutdown để bảo vệ dữ liệu.")
					}
					if b.onFlushErr != nil {
						b.onFlushErr(err)
					}
					return
				}
			case <-b.stopTickerCh:
				return
			case <-ctx.Done():
				return
			}
		}
	}()
}

// StopTimer dừng timer ticker an toàn
func (b *BatchFlusher) StopTimer() {
	b.stopOnce.Do(func() {
		if b.timerTicker != nil {
			b.timerTicker.Stop()
			close(b.stopTickerCh)
		}
	})
}

// AddEvent thêm 1 envelope (*contract.EventEnvelope hoặc *contract.KillEventEnvelope) vào bộ đệm
func (b *BatchFlusher) AddEvent(ctx context.Context, envelope interface{}) (int64, error) {
	b.mu.Lock()

	bytesLen := int64(300) // Khái toán dung lượng envelope
	b.rawEnvelopes = append(b.rawEnvelopes, envelope)
	b.rawBytes += bytesLen

	shouldFlush := len(b.rawEnvelopes) >= b.cfg.MaxBatchSize || b.rawBytes >= b.cfg.MaxBatchBytes
	b.mu.Unlock()

	if shouldFlush {
		return b.FlushRaw(ctx)
	}
	return 0, nil
}

// AddInvalid thêm 1 InvalidRecord vào bộ đệm DLQ
func (b *BatchFlusher) AddInvalid(ctx context.Context, invalid *contract.InvalidRecord) (int64, error) {
	b.mu.Lock()

	bytesLen := int64(len(invalid.SourceFile) + 200)
	b.invalidRecs = append(b.invalidRecs, invalid)
	b.invalidBytes += bytesLen

	shouldFlush := len(b.invalidRecs) >= b.cfg.MaxBatchSize || b.invalidBytes >= b.cfg.MaxBatchBytes
	b.mu.Unlock()

	if shouldFlush {
		return b.FlushInvalid(ctx)
	}
	return 0, nil
}

// FlushRaw thực thi phát raw envelopes sang Kafka với cơ chế Transactional Buffer
// CHỈ xóa tin nhắn khỏi buffer SAU KHI Kafka đã ACK thành công
func (b *BatchFlusher) FlushRaw(ctx context.Context) (int64, error) {
	b.mu.Lock()
	if len(b.rawEnvelopes) == 0 {
		b.mu.Unlock()
		return 0, nil
	}

	// Sao chép snapshot mảng buffer hiện tại — KHÔNG clear buffer trước khi gửi
	toSend := make([]interface{}, len(b.rawEnvelopes))
	copy(toSend, b.rawEnvelopes)
	b.mu.Unlock()

	if b.producer == nil {
		b.mu.Lock()
		b.rawEnvelopes = b.rawEnvelopes[:0]
		b.rawBytes = 0
		b.mu.Unlock()
		return int64(len(toSend)), nil
	}

	var sentCount int
	for _, item := range toSend {
		var err error
		switch env := item.(type) {
		case *contract.KillEventEnvelope:
			err = b.producer.ProduceKillEvent(ctx, env)
		case *contract.EventEnvelope:
			err = b.producer.ProduceEvent(ctx, env)
		default:
			err = fmt.Errorf("không hỗ trợ kiểu envelope: %T", item)
		}

		if err != nil {
			// Gửi thất bại: loại bỏ phần đã gửi thành công (sentCount), giữ nguyên phần chưa gửi trong buffer để retry
			b.mu.Lock()
			if sentCount > 0 {
				b.rawEnvelopes = b.rawEnvelopes[sentCount:]
			}
			b.mu.Unlock()
			return int64(sentCount), fmt.Errorf("fail-close trong batch raw produce tại message %d/%d: %w", sentCount+1, len(toSend), err)
		}
		sentCount++
	}

	// Gửi thành công toàn bộ batch: xóa đúng batch đã gửi khỏi buffer
	b.mu.Lock()
	b.rawEnvelopes = b.rawEnvelopes[sentCount:]
	if len(b.rawEnvelopes) == 0 {
		b.rawBytes = 0
	}
	b.mu.Unlock()

	count := int64(sentCount)
	if b.log != nil && count > 0 {
		batchIdx := b.batchCounter.Add(1)
		producedSoFar := b.totalProduced.Add(count)
		b.log.WithFields(logrus.Fields{
			"batch_index":     batchIdx,
			"batch_size":      count,
			"produced_so_far": producedSoFar,
			"timestamp":       time.Now().Format(time.RFC3339Nano),
		}).Info("🚀 [FLUSH RAW BATCH] Đã phát thành công Micro-Batch sang Kafka!")
	}

	return count, nil
}

// FlushInvalid thực thi phát InvalidRecords sang Kafka DLQ với Transactional Buffer
func (b *BatchFlusher) FlushInvalid(ctx context.Context) (int64, error) {
	b.mu.Lock()
	if len(b.invalidRecs) == 0 {
		b.mu.Unlock()
		return 0, nil
	}

	toSend := make([]*contract.InvalidRecord, len(b.invalidRecs))
	copy(toSend, b.invalidRecs)
	b.mu.Unlock()

	if b.producer == nil {
		b.mu.Lock()
		b.invalidRecs = b.invalidRecs[:0]
		b.invalidBytes = 0
		b.mu.Unlock()
		return int64(len(toSend)), nil
	}

	var sentCount int
	for _, inv := range toSend {
		if err := b.producer.ProduceInvalid(ctx, inv); err != nil {
			b.mu.Lock()
			if sentCount > 0 {
				b.invalidRecs = b.invalidRecs[sentCount:]
			}
			b.mu.Unlock()
			return int64(sentCount), fmt.Errorf("fail-close trong batch invalid produce tại message %d/%d: %w", sentCount+1, len(toSend), err)
		}
		sentCount++
	}

	b.mu.Lock()
	b.invalidRecs = b.invalidRecs[sentCount:]
	if len(b.invalidRecs) == 0 {
		b.invalidBytes = 0
	}
	b.mu.Unlock()

	return int64(sentCount), nil
}

// FlushAll thực thi Flush sạch cả 2 bộ đệm Raw và Invalid
func (b *BatchFlusher) FlushAll(ctx context.Context) error {
	if _, err := b.FlushRaw(ctx); err != nil {
		return err
	}
	if _, err := b.FlushInvalid(ctx); err != nil {
		return err
	}
	return nil
}
