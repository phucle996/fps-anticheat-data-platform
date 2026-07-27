package service

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"time"
)

// CheckpointState đại diện cho trạng thái tiến trình Replay lưu trữ trên MinIO S3
type CheckpointState struct {
	DatasetID                string    `json:"dataset_id"`                  // ID dataset đang replay
	SourceFile               string    `json:"source_file"`                 // Tên file CSV nguồn (vd: train_V2.csv)
	LastCompletedRecordIndex int64     `json:"last_completed_record_index"` // Chỉ số bản ghi cuối cùng đã phát thành công vào Kafka
	UpdatedAt                time.Time `json:"updated_at"`                  // Thời điểm lưu checkpoint (UTC)
}

// CheckpointStore định nghĩa interface thao tác lưu trữ Checkpoint State
type CheckpointStore interface {
	// Load nạp trạng thái Checkpoint hiện tại từ storage
	Load(ctx context.Context) (*CheckpointState, error)
	// Save lưu trạng thái Checkpoint mới nhất vào storage
	Save(ctx context.Context, state *CheckpointState) error
	// Reset xóa bỏ trạng thái Checkpoint cũ khỏi storage
	Reset(ctx context.Context) error
}

// MinIOCheckpointStore triển khai CheckpointStore lưu trữ file JSON trên MinIO S3 (Stateless Cloud-Native Pattern)
type MinIOCheckpointStore struct {
	minioCli  *MinIOClient // MinIO S3 Client
	objectKey string       // Key MinIO S3 (checkpoints/go-replay/state.json)
}

// NewMinIOCheckpointStore khởi tạo MinIOCheckpointStore
func NewMinIOCheckpointStore(minioCli *MinIOClient, objectKey string) *MinIOCheckpointStore {
	if objectKey == "" {
		objectKey = "checkpoints/go-replay/state.json"
	}
	return &MinIOCheckpointStore{
		minioCli:  minioCli,
		objectKey: objectKey,
	}
}

// Load đọc và giải mã file Checkpoint JSON từ MinIO S3
func (m *MinIOCheckpointStore) Load(ctx context.Context) (*CheckpointState, error) {
	// Kiểm tra xem file state.json đã tồn tại trên MinIO S3 chưa
	exists, err := m.minioCli.ObjectExists(ctx, m.objectKey)
	if err != nil {
		return nil, fmt.Errorf("kiểm tra checkpoint object thất bại: %w", err)
	}
	if !exists {
		// Chưa có checkpoint -> Trả về nil
		return nil, nil
	}

	// Tải stream JSON từ MinIO S3
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

// Reset xóa bỏ vật lý file state.json trên MinIO S3 khi người dùng yêu cầu Reset Checkpoint
func (m *MinIOCheckpointStore) Reset(ctx context.Context) error {
	exists, err := m.minioCli.ObjectExists(ctx, m.objectKey)
	if err != nil {
		return err
	}
	if exists {
		// Xóa vật lý file state.json trên MinIO S3
		return m.minioCli.RemoveObject(ctx, m.objectKey)
	}
	return nil
}

// Compile-time interface assertion kiểm tra MinIOCheckpointStore triển khai đúng CheckpointStore
var _ CheckpointStore = (*MinIOCheckpointStore)(nil)
