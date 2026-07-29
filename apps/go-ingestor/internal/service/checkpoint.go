package service

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"sync"
	"time"
)

// CheckpointState đại diện cho trạng thái tiến trình Replay lưu trữ trên MinIO S3
type CheckpointState struct {
	DatasetID             string    `json:"dataset_id"`               // ID dataset đang replay
	DatasetSHA256         string    `json:"dataset_sha256"`           // Mã băm SHA-256 của file zip nguồn
	SourceFile            string    `json:"source_file"`              // Tên file CSV nguồn (vd: kill_match_stats_final_0.csv)
	SchemaVersion         string    `json:"schema_version"`           // Phiên bản schema (vd: kill-event-v1)
	LastAckedRecordIndex  int64     `json:"last_acked_record_index"`  // Record index liên tục cao nhất đã được Kafka ACK thành công
	UpdatedAt             time.Time `json:"updated_at"`               // Thời điểm lưu checkpoint (UTC)
}

// CheckpointStore định nghĩa interface thao tác lưu trữ Checkpoint State
type CheckpointStore interface {
	Load(ctx context.Context) (*CheckpointState, error)
	Save(ctx context.Context, state *CheckpointState) error
	Reset(ctx context.Context) error
}

// MinIOCheckpointStore triển khai CheckpointStore với S3 Key theo namespace dataset_sha256
type MinIOCheckpointStore struct {
	minioCli  *MinIOClient // MinIO S3 Client
	objectKey string       // Object key cố định hoặc namespace
}

// NewMinIOCheckpointStore khởi tạo MinIOCheckpointStore với S3 key theo namespace dataset_sha256
func NewMinIOCheckpointStore(minioCli *MinIOClient, datasetSHA256, sourceFile string) *MinIOCheckpointStore {
	key := fmt.Sprintf("checkpoints/%s/%s/go-replay.json", datasetSHA256, sourceFile)
	return &MinIOCheckpointStore{
		minioCli:  minioCli,
		objectKey: key,
	}
}

// Load đọc và giải mã file Checkpoint JSON từ MinIO S3
func (m *MinIOCheckpointStore) Load(ctx context.Context) (*CheckpointState, error) {
	exists, err := m.minioCli.ObjectExists(ctx, m.objectKey)
	if err != nil {
		return nil, fmt.Errorf("kiểm tra checkpoint object thất bại: %w", err)
	}
	if !exists {
		return nil, nil
	}

	stream, err := m.minioCli.DownloadStream(ctx, m.objectKey)
	if err != nil {
		return nil, fmt.Errorf("tải stream checkpoint thất bại: %w", err)
	}
	defer stream.Close()

	var state CheckpointState
	if err := json.NewDecoder(stream).Decode(&state); err != nil {
		return nil, fmt.Errorf("decode Checkpoint JSON thất bại: %w", err)
	}

	return &state, nil
}

// Save mã hóa và lưu CheckpointState mới nhất lên MinIO S3
func (m *MinIOCheckpointStore) Save(ctx context.Context, state *CheckpointState) error {
	state.UpdatedAt = time.Now().UTC()
	dataBytes, err := json.MarshalIndent(state, "", "  ")
	if err != nil {
		return fmt.Errorf("encode Checkpoint JSON thất bại: %w", err)
	}

	reader := bytes.NewReader(dataBytes)
	err = m.minioCli.UploadStream(ctx, m.objectKey, reader, int64(len(dataBytes)), "application/json")
	if err != nil {
		return fmt.Errorf("lưu Checkpoint lên MinIO S3 thất bại: %w", err)
	}
	return nil
}

// Reset xóa bỏ file state.json trên MinIO S3
func (m *MinIOCheckpointStore) Reset(ctx context.Context) error {
	exists, err := m.minioCli.ObjectExists(ctx, m.objectKey)
	if err != nil {
		return err
	}
	if exists {
		return m.minioCli.RemoveObject(ctx, m.objectKey)
	}
	return nil
}

// ContiguousAckTracker quản lý việc theo dõi các record index đã được Kafka ACK
// Tính toán chính xác highest contiguous ACKed record index (chống out-of-order ack)
type ContiguousAckTracker struct {
	mu           sync.Mutex
	lastAcked    int64
	pendingAcks  map[int64]bool
}

// NewContiguousAckTracker khởi tạo tracker từ index khởi điểm (lastAckedIndex)
func NewContiguousAckTracker(initialIndex int64) *ContiguousAckTracker {
	return &ContiguousAckTracker{
		lastAcked:   initialIndex,
		pendingAcks: make(map[int64]bool),
	}
}

// RecordAck ghi nhận 1 record index đã được Kafka ACK và tính toán lastAcked mới
func (t *ContiguousAckTracker) RecordAck(recordIndex int64) int64 {
	t.mu.Lock()
	defer t.mu.Unlock()

	t.pendingAcks[recordIndex] = true

	// Tăng lastAcked liên tục khi các index tiếp theo đã có trong pendingAcks
	for t.pendingAcks[t.lastAcked+1] {
		delete(t.pendingAcks, t.lastAcked+1)
		t.lastAcked++
	}

	return t.lastAcked
}

// GetLastContiguousAcked trả về record index liên tục cao nhất hiện tại
func (t *ContiguousAckTracker) GetLastContiguousAcked() int64 {
	t.mu.Lock()
	defer t.mu.Unlock()
	return t.lastAcked
}

// Compile-time interface assertion
var _ CheckpointStore = (*MinIOCheckpointStore)(nil)
