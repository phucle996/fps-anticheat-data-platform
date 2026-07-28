package service_test

import (
	"context"
	"testing"
	"time"

	"pubg-anti-cheat/go-ingestor/internal/contract"
	"pubg-anti-cheat/go-ingestor/internal/service"
)

// TestBatchBuffer_RecordCountBoundary kiểm tra trigger Flush khi bộ đệm đạt MaxBatchSize
func TestBatchBuffer_RecordCountBoundary(t *testing.T) {
	mockProd := &MockProducer{}

	cfg := service.BatchConfig{
		MaxBatchSize:  2, // Batch tối đa 2 bản ghi
		MaxBatchBytes: 100000,
		FlushInterval: 1 * time.Second,
	}

	flusher := service.NewBatchFlusher(cfg, mockProd)

	env1 := &contract.EventEnvelope{EventID: "ev-1", MatchID: "m-1"}
	env2 := &contract.EventEnvelope{EventID: "ev-2", MatchID: "m-1"}

	// Thêm bản ghi 1 chưa đạt boundary
	count, err := flusher.AddEvent(context.Background(), env1)
	if err != nil {
		t.Fatalf("AddEvent 1 thất bại: %v", err)
	}
	if count != 0 {
		t.Errorf("Kỳ vọng chưa flush (count = 0), nhận được = %d", count)
	}

	// Thêm bản ghi 2 đạt MaxBatchSize = 2 -> Tự động Trigger Flush
	count, err = flusher.AddEvent(context.Background(), env2)
	if err != nil {
		t.Fatalf("AddEvent 2 thất bại: %v", err)
	}
	if count != 2 {
		t.Errorf("Kỳ vọng đã flush 2 bản ghi, nhận được = %d", count)
	}
}

// TestBatchBuffer_ByteSizeBoundary kiểm tra trigger Flush khi bộ đệm đạt MaxBatchBytes
func TestBatchBuffer_ByteSizeBoundary(t *testing.T) {
	mockProd := &MockProducer{}

	cfg := service.BatchConfig{
		MaxBatchSize:  1000,
		MaxBatchBytes: 150, // Ngưỡng dung lượng byte nhỏ 150 bytes
		FlushInterval: 1 * time.Second,
	}

	flusher := service.NewBatchFlusher(cfg, mockProd)

	env1 := &contract.EventEnvelope{EventID: "ev-large-1", MatchID: "m-100", PlayerID: "player-long-id-12345"}

	count, err := flusher.AddEvent(context.Background(), env1)
	if err != nil {
		t.Fatalf("AddEvent thất bại: %v", err)
	}
	if count != 1 {
		t.Errorf("Kỳ vọng kích thước byte (~284 bytes) vượt ngưỡng 150 bytes đã trigger flush 1 bản ghi, nhận được = %d", count)
	}
}

// TestBatchBuffer_TimerFlush kiểm tra trigger Flush theo thời gian Timer Interval
func TestBatchBuffer_TimerFlush(t *testing.T) {
	mockProd := &MockProducer{}

	cfg := service.BatchConfig{
		MaxBatchSize:  100, // Ngưỡng size lớn để không flush theo count
		MaxBatchBytes: 100000,
		FlushInterval: 20 * time.Millisecond, // Timer flush 20ms
	}

	flusher := service.NewBatchFlusher(cfg, mockProd)
	ctx := context.Background()

	flusher.StartTimer(ctx)
	defer flusher.StopTimer()

	env1 := &contract.EventEnvelope{EventID: "ev-timer", MatchID: "m-1"}
	_, err := flusher.AddEvent(ctx, env1)
	if err != nil {
		t.Fatalf("AddEvent thất bại: %v", err)
	}

	// Đợi 50ms để timer 20ms kích hoạt trigger flush
	time.Sleep(50 * time.Millisecond)

	if len(mockProd.GetProducedEvents()) != 1 {
		t.Errorf("Kỳ vọng Timer Flush phát 1 bản ghi vào Kafka, nhận được: %d", len(mockProd.GetProducedEvents()))
	}
}

// TestBatchBuffer_EOFFlush kiểm tra FlushAll phát sạch dữ liệu khi chạm EOF / Shutdown
func TestBatchBuffer_EOFFlush(t *testing.T) {
	mockProd := &MockProducer{}

	cfg := service.BatchConfig{
		MaxBatchSize:  100,
		MaxBatchBytes: 100000,
		FlushInterval: 10 * time.Minute,
	}

	flusher := service.NewBatchFlusher(cfg, mockProd)
	ctx := context.Background()

	env1 := &contract.EventEnvelope{EventID: "ev-eof", MatchID: "m-1"}
	_, err := flusher.AddEvent(ctx, env1)
	if err != nil {
		t.Fatalf("AddEvent thất bại: %v", err)
	}

	// Gọi FlushAll trực tiếp giả lập EOF / Shutdown
	err = flusher.FlushAll(ctx)
	if err != nil {
		t.Fatalf("FlushAll thất bại: %v", err)
	}

	if len(mockProd.producedEvents) != 1 {
		t.Errorf("Kỳ vọng FlushAll đẩy sạch 1 bản ghi tồn đọng, nhận được: %d", len(mockProd.producedEvents))
	}
}
