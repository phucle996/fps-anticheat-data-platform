package service

import (
	"context"
	"encoding/json"
	"fmt"
	"sync"
	"time"

	"pubg-anti-cheat/go-ingestor/internal/contract"
)

// BatchConfig định nghĩa cấu hình điều khiển vi luồng Micro-Batching
type BatchConfig struct {
	MaxBatchSize  int           // Số bản ghi tối đa trước khi trigger flush (vd: 100)
	MaxBatchBytes int64         // Dung lượng byte tối đa trước khi trigger flush (vd: 65,536 bytes = 64KB)
	FlushInterval time.Duration // Khoảng thời gian timer flush nhịp định kỳ (vd: 10ms)
}

// BatchFlusher quản lý bộ đệm Micro-Batching hai luồng (Raw Event và Invalid DLQ)
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
}

// NewBatchFlusher khởi tạo BatchFlusher với cấu hình batch quy định
func NewBatchFlusher(cfg BatchConfig, producer Producer) *BatchFlusher {
	if cfg.MaxBatchSize <= 0 {
		cfg.MaxBatchSize = 100
	}
	if cfg.MaxBatchBytes <= 0 {
		cfg.MaxBatchBytes = 65536
	}
	if cfg.FlushInterval <= 0 {
		cfg.FlushInterval = 10 * time.Millisecond
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

// StartTimer kích hoạt vòng lặp Flush nhịp định kỳ theo thời gian (Timer Flush)
func (b *BatchFlusher) StartTimer(ctx context.Context) {
	b.timerTicker = time.NewTicker(b.cfg.FlushInterval)
	go func() {
		for {
			select {
			case <-b.timerTicker.C:
				// Flush nhịp định kỳ cả 2 bộ đệm nếu có chứa dữ liệu
				_ = b.FlushAll(ctx)
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
	if b.timerTicker != nil {
		b.timerTicker.Stop()
		close(b.stopTickerCh)
	}
}

// AddEvent thêm 1 EventEnvelope hợp lệ vào bộ đệm, tự động Flush nếu đạt ranh giới (Count hoặc Bytes)
func (b *BatchFlusher) AddEvent(ctx context.Context, envelope *contract.EventEnvelope) (int64, error) {
	b.mu.Lock()

	// Tính toán ước lượng kích thước byte JSON của envelope
	bytesLen := int64(len(envelope.EventID) + len(envelope.MatchID) + len(envelope.PlayerID) + 250)
	b.rawEnvelopes = append(b.rawEnvelopes, envelope)
	b.rawBytes += bytesLen

	// Kiểm tra xem đã đạt ranh giới Record Count hoặc Max Bytes hay chưa
	shouldFlush := len(b.rawEnvelopes) >= b.cfg.MaxBatchSize || b.rawBytes >= b.cfg.MaxBatchBytes
	b.mu.Unlock()

	if shouldFlush {
		return b.FlushRaw(ctx)
	}
	return 0, nil
}

// AddInvalid thêm 1 InvalidRecord vào bộ đệm DLQ, tự động Flush nếu đạt ranh giới
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

// FlushRaw thực thi phát toàn bộ EventEnvelope trong bộ đệm sang Kafka (Trả về số bản ghi phát thành công)
func (b *BatchFlusher) FlushRaw(ctx context.Context) (int64, error) {
	b.mu.Lock()
	if len(b.rawEnvelopes) == 0 {
		b.mu.Unlock()
		return 0, nil
	}

	// Copy danh sách bản ghi cần phát và reset bộ đệm
	toSend := b.rawEnvelopes
	b.rawEnvelopes = make([]*contract.EventEnvelope, 0, b.cfg.MaxBatchSize)
	b.rawBytes = 0
	b.mu.Unlock()

	if b.producer == nil {
		// Ở chế độ Dry-Run không có Producer, trả về số bản ghi giả lập thành công
		return int64(len(toSend)), nil
	}

	// Phát từng tin nhắn độc lập vào Kafka (Không bọc thành JSON Array)
	var count int64
	for _, env := range toSend {
		if err := b.producer.ProduceEvent(ctx, env); err != nil {
			return count, fmt.Errorf("fail-close trong batch raw produce: %w", err)
		}
		count++
	}

	return count, nil
}

// FlushInvalid thực thi phát toàn bộ InvalidRecord trong bộ đệm sang Kafka DLQ
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

// Ensure Producer interface check
var _ json.Marshaler = (json.Marshaler)(nil)
