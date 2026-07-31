package service_test

import (
	"context"
	"errors"
	"sync"
	"testing"

	"go-ingestor/internal/contract"
	"go-ingestor/internal/service"
)

// MockProducer giả lập Kafka Producer triển khai service.Producer interface (Thread-Safe Mutex Active)
type MockProducer struct {
	producedEvents   []*contract.EventEnvelope
	producedInvalids []*contract.InvalidRecord
	shouldFail       bool       // Cờ giả lập lỗi Fail-Close từ Kafka
	mu               sync.Mutex // Mutex bảo vệ truy cập mảng khi chạy đa luồng
}

// Ensure interface implementation check
var _ service.Producer = (*MockProducer)(nil)

func (m *MockProducer) ProduceEvent(ctx context.Context, envelope *contract.EventEnvelope) error {
	m.mu.Lock()
	defer m.mu.Unlock()

	if m.shouldFail {
		return errors.New("kafka cluster unavailable (mock error)")
	}
	m.producedEvents = append(m.producedEvents, envelope)
	return nil
}

func (m *MockProducer) ProduceKillEvent(ctx context.Context, envelope *contract.KillEventEnvelope) error {
	m.mu.Lock()
	defer m.mu.Unlock()

	if m.shouldFail {
		return errors.New("kafka cluster unavailable (mock error)")
	}
	// Chuyển sang EventEnvelope mock nếu cần
	return nil
}

func (m *MockProducer) ProduceInvalid(ctx context.Context, invalid *contract.InvalidRecord) error {
	m.mu.Lock()
	defer m.mu.Unlock()

	if m.shouldFail {
		return errors.New("kafka DLQ topic full (mock error)")
	}
	m.producedInvalids = append(m.producedInvalids, invalid)
	return nil
}

func (m *MockProducer) GetProducedEvents() []*contract.EventEnvelope {
	m.mu.Lock()
	defer m.mu.Unlock()
	res := make([]*contract.EventEnvelope, len(m.producedEvents))
	copy(res, m.producedEvents)
	return res
}

func (m *MockProducer) Close() error {
	return nil
}

// TestProducer_ProduceEventSuccess kiểm tra phát tin nhắn hợp lệ thành công qua MockProducer
func TestProducer_ProduceEventSuccess(t *testing.T) {
	mockProd := &MockProducer{}

	envelope := &contract.EventEnvelope{
		EventID:  "event-123",
		MatchID:  "match-456",
		PlayerID: "player-789",
	}

	err := mockProd.ProduceEvent(context.Background(), envelope)
	if err != nil {
		t.Fatalf("ProduceEvent trả về lỗi bất ngờ: %v", err)
	}

	if len(mockProd.producedEvents) != 1 {
		t.Errorf("Kỳ vọng 1 event được phát, nhận được: %d", len(mockProd.producedEvents))
	}
	if mockProd.producedEvents[0].MatchID != "match-456" {
		t.Errorf("MatchID không trùng khớp: %s", mockProd.producedEvents[0].MatchID)
	}
}

// TestProducer_FailCloseOnError kiểm tra tính năng Fail-Close khi Kafka gặp sự cố
func TestProducer_FailCloseOnError(t *testing.T) {
	mockProd := &MockProducer{shouldFail: true}

	envelope := &contract.EventEnvelope{
		EventID:  "event-123",
		MatchID:  "match-456",
		PlayerID: "player-789",
	}

	err := mockProd.ProduceEvent(context.Background(), envelope)
	if err == nil {
		t.Fatalf("Kỳ vọng trả về lỗi Fail-Close từ Kafka nhưng nhận được nil")
	}

	invalid := &contract.InvalidRecord{
		SourceFile:  "train_V2.csv",
		RecordIndex: 5,
	}

	errDLQ := mockProd.ProduceInvalid(context.Background(), invalid)
	if errDLQ == nil {
		t.Fatalf("Kỳ vọng trả về lỗi Fail-Close khi phát DLQ nhưng nhận được nil")
	}
}
