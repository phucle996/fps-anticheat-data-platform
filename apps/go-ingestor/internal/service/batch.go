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

// BatchConfig chứa cấu hình bộ đệm Micro-Batching Flusher
type BatchConfig struct {
	MaxBatchSize  int           // Số lượng bản ghi tối đa trong 1 batch (vd: 400)
	MaxBatchBytes int64         // Dung lượng bytes tối đa trong 1 batch (vd: 1MB = 1048576)
	FlushInterval time.Duration // Thời gian tối đa giữ batch trước khi tự động flush (vd: 500ms)
}

// BatchFlusher thực thi gom tin nhắn thành từng Micro-Batch để gửi vào Kafka
// Đảm bảo an toàn thread-safe 100%, bảo vệ dữ liệu chống race-condition giữa ticker timer và main stream loop
type BatchFlusher struct {
	cfg            BatchConfig
	producer       Producer
	mu             sync.Mutex // Mutex bảo vệ truy cập buffer slices
	flushMu        sync.Mutex // Mutex đảm bảo chỉ 1 tiến trình FlushRaw/FlushInvalid thực thi tại một thời điểm
	rawEnvelopes   []interface{}
	rawBytes       int64
	invalidRecs    []*contract.InvalidRecord
	invalidBytes   int64
	timerTicker    *time.Ticker
	stopTickerCh   chan struct{}
	stopOnce       sync.Once
	log            *logrus.Entry
	errHandler     func(error)
	batchCounter   atomic.Int64
	totalProduced  atomic.Int64
}

// NewBatchFlusher khởi tạo BatchFlusher với cấu hình mặc định an toàn
func NewBatchFlusher(cfg BatchConfig, producer Producer) *BatchFlusher {
	if cfg.MaxBatchSize <= 0 {
		cfg.MaxBatchSize = 400
	}
	if cfg.MaxBatchBytes <= 0 {
		cfg.MaxBatchBytes = 1048576 // 1MB
	}
	if cfg.FlushInterval <= 0 {
		cfg.FlushInterval = 500 * time.Millisecond
	}

	flusher := &BatchFlusher{
		cfg:          cfg,
		producer:     producer,
		rawEnvelopes: make([]interface{}, 0, cfg.MaxBatchSize),
		invalidRecs:  make([]*contract.InvalidRecord, 0, cfg.MaxBatchSize),
		stopTickerCh: make(chan struct{}),
	}

	return flusher
}

// SetLogger thiết lập logger cho Flusher
func (b *BatchFlusher) SetLogger(log *logrus.Entry) {
	b.log = log
}

// SetErrorHandler thiết lập callback xử lý lỗi Fail-Fast cho background ticker timer
func (b *BatchFlusher) SetErrorHandler(fn func(error)) {
	b.errHandler = fn
}

// StartTimer khởi chạy background goroutine tự động flush dữ liệu định kỳ theo FlushInterval
func (b *BatchFlusher) StartTimer(ctx context.Context) {
	b.timerTicker = time.NewTicker(b.cfg.FlushInterval)
	go func() {
		for {
			select {
			case <-b.timerTicker.C:
				flushCtx, cancel := context.WithTimeout(ctx, 10*time.Second)
				if err := b.FlushAll(flushCtx); err != nil {
					if b.errHandler != nil {
						b.errHandler(err)
					}
				}
				cancel()
			case <-b.stopTickerCh:
				return
			case <-ctx.Done():
				return
			}
		}
	}()
}

// StopTimer dừng background ticker timer an toàn
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

// FlushRaw thực thi phát raw envelopes sang Kafka với cơ chế Mutex Isolation chống Race Condition
// CHỈ xóa tin nhắn khỏi buffer SAU KHI Kafka đã ACK thành công
func (b *BatchFlusher) FlushRaw(ctx context.Context) (int64, error) {
	b.flushMu.Lock()
	defer b.flushMu.Unlock()

	b.mu.Lock()
	if len(b.rawEnvelopes) == 0 {
		b.mu.Unlock()
		return 0, nil
	}

	// Tráo đổi atomic snapshot buffer để các thread AddEvent khác tiếp tục ghi mà không bị tranh chấp
	toSend := b.rawEnvelopes
	b.rawEnvelopes = make([]interface{}, 0, b.cfg.MaxBatchSize)
	b.rawBytes = 0
	b.mu.Unlock()

	if b.producer == nil {
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
			// Nếu gửi thất bại tại giữa chừng: đưa phần chưa gửi thành công quay trở lại buffer b.rawEnvelopes để retry
			b.mu.Lock()
			unSent := toSend[sentCount:]
			b.rawEnvelopes = append(unSent, b.rawEnvelopes...)
			b.mu.Unlock()
			return int64(sentCount), fmt.Errorf("fail-close trong batch raw produce tại message %d/%d: %w", sentCount+1, len(toSend), err)
		}
		sentCount++
	}

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
	b.flushMu.Lock()
	defer b.flushMu.Unlock()

	b.mu.Lock()
	if len(b.invalidRecs) == 0 {
		b.mu.Unlock()
		return 0, nil
	}

	toSend := b.invalidRecs
	b.invalidRecs = make([]*contract.InvalidRecord, 0, b.cfg.MaxBatchSize)
	b.invalidBytes = 0
	b.mu.Unlock()

	if b.producer == nil {
		return int64(len(toSend)), nil
	}

	var sentCount int
	for _, inv := range toSend {
		if err := b.producer.ProduceInvalid(ctx, inv); err != nil {
			b.mu.Lock()
			unSent := toSend[sentCount:]
			b.invalidRecs = append(unSent, b.invalidRecs...)
			b.mu.Unlock()
			return int64(sentCount), fmt.Errorf("fail-close trong batch invalid produce tại message %d/%d: %w", sentCount+1, len(toSend), err)
		}
		sentCount++
	}

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
