package service

import (
	"context"
	"encoding/json"
	"fmt"
	"sync"
	"time"

	"pubg-anti-cheat/go-ingestor/internal/contract"
)

// BatchConfig định nghĩa cấu hình Micro-Batching (Dành riêng cho Go Ingestor tối ưu I/O & băng thông TCP)
type BatchConfig struct {
	MaxBatchSize  int           // Số bản ghi tối đa trong micro-batch (Mặc định: 20 tin nhắn)
	MaxBatchBytes int64         // Kích thước byte tối đa trong micro-batch (Mặc định: 16KB = 16,384 bytes)
	FlushInterval time.Duration // Nhịp timer flush cân bằng (Mặc định: 500ms)
}

// BatchFlusher quản lý bộ đệm Micro-Batching (Mục đích: Tiết kiệm syscall I/O và tối ưu nén Zstd sang Kafka)
type BatchFlusher struct {
	cfg          BatchConfig
	producer     Producer
	rawEnvelopes []*contract.EventEnvelope // Bộ đệm mảng sự kiện hợp lệ
	rawBytes     int64                     // Dung lượng byte tích lũy của rawBuffer
	invalidRecs  []*contract.InvalidRecord // Bộ đệm mảng sự kiện vi phạm
	invalidBytes int64                     // Dung lượng byte tích lũy của invalidBuffer
	mu           sync.Mutex                // Mutex bảo vệ truy cập đồng thời
	timerTicker  *time.Ticker              // Timer ticker nhịp flush định kỳ
	stopTickerCh chan struct{}             // Channel báo dừng ticker
	stopOnce     sync.Once                 // Guard đảm bảo channel chỉ close đúng 1 lần (Thread-safe)
}

// NewBatchFlusher khởi tạo BatchFlusher với cấu hình mặc định cân bằng (500ms Flush Interval)
func NewBatchFlusher(cfg BatchConfig, producer Producer) *BatchFlusher {
	// Micro-batch 20 tin nhắn (đảm bảo latency thấp, nhường batch lớn cho Rust Engine)
	if cfg.MaxBatchSize <= 0 {
		cfg.MaxBatchSize = 20
	}
	// Micro-batch 16KB để tối ưu Zstd compression mà không tiêu tốn RAM
	if cfg.MaxBatchBytes <= 0 {
		cfg.MaxBatchBytes = 16384
	}
	// Tần số Flush 500ms cân bằng giữa CPU overhead và latency
	if cfg.FlushInterval <= 0 {
		cfg.FlushInterval = 500 * time.Millisecond
	}

	return &BatchFlusher{
		cfg:          cfg,
		producer:     producer,
		rawEnvelopes: make([]*contract.EventEnvelope, 0, cfg.MaxBatchSize),
		rawBytes:     0,
		invalidRecs:  make([]*contract.InvalidRecord, 0, cfg.MaxBatchSize),
		invalidBytes: 0,
		stopTickerCh: make(chan struct{}),
	}
}

// StartTimer kích hoạt vòng lặp Flush nhịp định kỳ 500ms
func (b *BatchFlusher) StartTimer(ctx context.Context) {
	b.timerTicker = time.NewTicker(b.cfg.FlushInterval)
	go func() {
		for {
			select {
			case <-b.timerTicker.C:
				// Flush nhịp định kỳ 500ms nếu bộ đệm có dữ liệu chưa gửi
				_ = b.FlushAll(ctx)
			case <-b.stopTickerCh:
				return
			case <-ctx.Done():
				return
			}
		}
	}()
}

// StopTimer dừng timer ticker an toàn, chống panic khi gọi trùng lặp (sync.Once)
func (b *BatchFlusher) StopTimer() {
	b.stopOnce.Do(func() {
		if b.timerTicker != nil {
			b.timerTicker.Stop()
			close(b.stopTickerCh)
		}
	})
}

// AddEvent thêm 1 EventEnvelope hợp lệ vào bộ đệm micro-batch, tự động Flush khi đủ 20 tin hoặc 16KB
func (b *BatchFlusher) AddEvent(ctx context.Context, envelope *contract.EventEnvelope) (int64, error) {
	b.mu.Lock()

	bytesLen := int64(len(envelope.EventID) + len(envelope.MatchID) + len(envelope.PlayerID) + 250)
	b.rawEnvelopes = append(b.rawEnvelopes, envelope)
	b.rawBytes += bytesLen

	shouldFlush := len(b.rawEnvelopes) >= b.cfg.MaxBatchSize || b.rawBytes >= b.cfg.MaxBatchBytes
	b.mu.Unlock()

	if shouldFlush {
		return b.FlushRaw(ctx)
	}
	return 0, nil
}

// AddInvalid thêm 1 InvalidRecord vào bộ đệm micro-batch DLQ
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

// FlushRaw thực thi phát mảng micro-batch EventEnvelope sang Kafka
func (b *BatchFlusher) FlushRaw(ctx context.Context) (int64, error) {
	b.mu.Lock()
	if len(b.rawEnvelopes) == 0 {
		b.mu.Unlock()
		return 0, nil
	}

	toSend := b.rawEnvelopes
	b.rawEnvelopes = make([]*contract.EventEnvelope, 0, b.cfg.MaxBatchSize)
	b.rawBytes = 0
	b.mu.Unlock()

	if b.producer == nil {
		return int64(len(toSend)), nil
	}

	var count int64
	for _, env := range toSend {
		if err := b.producer.ProduceEvent(ctx, env); err != nil {
			return count, fmt.Errorf("fail-close trong batch raw produce: %w", err)
		}
		count++
	}

	return count, nil
}

// FlushInvalid thực thi phát mảng micro-batch InvalidRecord sang Kafka DLQ
func (b *BatchFlusher) FlushInvalid(ctx context.Context) (int64, error) {
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

	var count int64
	for _, inv := range toSend {
		if err := b.producer.ProduceInvalid(ctx, inv); err != nil {
			return count, fmt.Errorf("fail-close trong batch invalid produce: %w", err)
		}
		count++
	}

	return count, nil
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

// Compile-time interface assertion
var _ json.Marshaler = (json.Marshaler)(nil)
