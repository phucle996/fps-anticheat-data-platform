package service_test

import (
	"context"
	"testing"
	"time"

	"pubg-anti-cheat/go-ingestor/internal/service"
)

// MockCheckpointStore giả lập CheckpointStore phục vụ Unit Test
type MockCheckpointStore struct {
	state *service.CheckpointState
}

func (m *MockCheckpointStore) Load(ctx context.Context) (*service.CheckpointState, error) {
	return m.state, nil
}

func (m *MockCheckpointStore) Save(ctx context.Context, state *service.CheckpointState) error {
	m.state = state
	return nil
}

func (m *MockCheckpointStore) Reset(ctx context.Context) error {
	m.state = nil
	return nil
}

// TestCheckpointStore_SaveAndLoad kiểm tra nạp và lưu CheckpointState
func TestCheckpointStore_SaveAndLoad(t *testing.T) {
	store := &MockCheckpointStore{}
	ctx := context.Background()

	state := &service.CheckpointState{
		DatasetID:                "test-dataset",
		SourceFile:               "train_V2.csv",
		LastCompletedRecordIndex: 50,
		UpdatedAt:                time.Now().UTC(),
	}

	err := store.Save(ctx, state)
	if err != nil {
		t.Fatalf("Save checkpoint thất bại: %v", err)
	}

	loaded, err := store.Load(ctx)
	if err != nil {
		t.Fatalf("Load checkpoint thất bại: %v", err)
	}

	if loaded == nil || loaded.LastCompletedRecordIndex != 50 {
		t.Errorf("LastCompletedRecordIndex không chính xác: %+v", loaded)
	}
}

// TestCheckpointStore_ResetState kiểm tra tính năng Reset Checkpoint
func TestCheckpointStore_ResetState(t *testing.T) {
	store := &MockCheckpointStore{
		state: &service.CheckpointState{
			DatasetID:                "test-dataset",
			LastCompletedRecordIndex: 100,
		},
	}

	err := store.Reset(context.Background())
	if err != nil {
		t.Fatalf("Reset checkpoint thất bại: %v", err)
	}

	loaded, _ := store.Load(context.Background())
	if loaded != nil {
		t.Errorf("Kỳ vọng Checkpoint đã bị xóa (nil), nhận được: %+v", loaded)
	}
}
