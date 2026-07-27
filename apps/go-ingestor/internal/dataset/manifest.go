package dataset

import (
	"encoding/json"
	"fmt"
	"time"
)

// DatasetManifest định nghĩa cấu trúc lưu trữ metadata trạng thái của Dataset trên MinIO S3
type DatasetManifest struct {
	DatasetID       string    `json:"dataset_id"`       // ID duy nhất của dataset
	Provider        string    `json:"provider"`         // Nguồn cung cấp (kaggle | local)
	Slug            string    `json:"slug"`             // Kaggle slug
	ArchivePath     string    `json:"archive_path"`     // MinIO S3 Object Key của archive zip
	ArchiveChecksum string    `json:"archive_checksum"` // Chuỗi mã SHA-256 của archive zip
	ExtractedPath   string    `json:"extracted_path"`   // MinIO S3 Object Key của file CSV chính
	SelectedFile    string    `json:"selected_file"`    // Tên file CSV chính
	TotalRecords    int64     `json:"total_records"`    // Tổng số bản ghi (nếu có)
	Status          string    `json:"status"`           // Trạng thái (ready | syncing | failed)
	CreatedAt       time.Time `json:"created_at"`       // Thời gian khởi tạo manifest UTC
}

// NewDatasetManifest khởi tạo một đối tượng DatasetManifest mới chuẩn hóa
func NewDatasetManifest(datasetID, slug, archivePath, checksum, extractedPath, selectedFile string) *DatasetManifest {
	return &DatasetManifest{
		DatasetID:       datasetID,
		Provider:        "kaggle",
		Slug:            slug,
		ArchivePath:     archivePath,
		ArchiveChecksum: checksum,
		ExtractedPath:   extractedPath,
		SelectedFile:    selectedFile,
		Status:          "ready",
		CreatedAt:       time.Now().UTC(),
	}
}

// ToJSON chuyển đổi struct Manifest thành mảng JSON bytes
func (m *DatasetManifest) ToJSON() ([]byte, error) {
	bytes, err := json.MarshalIndent(m, "", "  ")
	if err != nil {
		return nil, fmt.Errorf("không thể serialize DatasetManifest sang JSON: %w", err)
	}
	return bytes, nil
}
