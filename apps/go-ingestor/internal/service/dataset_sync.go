package service

import (
	"archive/zip"
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"io"
	"path/filepath"
	"strings"
	"time"

	"github.com/sirupsen/logrus"

	"pubg-anti-cheat/go-ingestor/internal/config"
)

// DatasetManifest định nghĩa cấu trúc lưu thông tin dataset metadata trên MinIO S3
type DatasetManifest struct {
	DatasetID       string    `json:"dataset_id"`       // ID duy nhất (vd: kaggle-catchmeifyoucan-pubg-finish-placement-prediction)
	DatasetSlug     string    `json:"dataset_slug"`     // Slug Kaggle (catchmeifyoucan/pubg-finish-placement-prediction)
	ArchivePath     string    `json:"archive_path"`     // Key MinIO của file zip archive
	ArchiveChecksum string    `json:"archive_checksum"` // SHA-256 Checksum của file zip
	ExtractedPath   string    `json:"extracted_path"`   // Key MinIO của file CSV đã giải nén
	SelectedFile    string    `json:"selected_file"`    // Tên file CSV được chọn (train_V2.csv)
	DownloadedAt    time.Time `json:"downloaded_at"`    // Thời điểm đồng bộ (UTC)
}

// ExtractedFile lưu thông tin file CSV đã giải nén từ bộ nhớ
type ExtractedFile struct {
	Filename string
	Size     int64
	Content  io.Reader
}

// DatasetSyncService điều phối usecase tải và đồng bộ Dataset từ Kaggle lên MinIO S3
type DatasetSyncService struct {
	cfg      *config.Config
	minioCli *MinIOClient
	log      *logrus.Entry
}

// NewDatasetSyncService khởi tạo DatasetSyncService
func NewDatasetSyncService(cfg *config.Config, log *logrus.Entry) (*DatasetSyncService, error) {
	minioCli, err := NewMinIOClient(
		cfg.MinIOEndpoint,
		cfg.MinIOAccessKey,
		cfg.MinIOSecretKey,
		cfg.MinIOBucket,
		cfg.MinIOUseSSL,
	)
	if err != nil {
		return nil, fmt.Errorf("không thể kết nối MinIO Storage: %w", err)
	}

	return &DatasetSyncService{
		cfg:      cfg,
		minioCli: minioCli,
		log:      log,
	}, nil
}

// Run thực thi usecase đồng bộ dataset
func (s *DatasetSyncService) Run(ctx context.Context) error {
	s.log.Info("Khởi động Use Case Dataset Sync (Flat Architecture)...")

	// 1. Đảm bảo bucket chính đã tồn tại
	if err := s.minioCli.EnsureBucketExists(ctx); err != nil {
		return fmt.Errorf("đảm bảo MinIO bucket tồn tại thất bại: %w", err)
	}

	// 2. Định nghĩa các Object Keys trên MinIO S3
	cleanSlug := strings.ReplaceAll(s.cfg.DatasetSlug, "/", "-")
	archiveObjectKey := fmt.Sprintf("archives/%s/dataset.zip", cleanSlug)
	extractedObjectKey := fmt.Sprintf("raw-sources/%s/%s", cleanSlug, s.cfg.SelectedFile)
	manifestObjectKey := "manifests/dataset-manifest.json"

	// 3. Kiểm tra xem dataset đã tồn tại chưa nếu không bật ForceDownload
	if !s.cfg.ForceDownload {
		manifestExists, _ := s.minioCli.ObjectExists(ctx, manifestObjectKey)
		extractedExists, _ := s.minioCli.ObjectExists(ctx, extractedObjectKey)

		if manifestExists && extractedExists {
			s.log.WithFields(logrus.Fields{
				"manifest":      manifestObjectKey,
				"extracted_csv": extractedObjectKey,
			}).Info("Dataset đã tồn tại đầy đủ trên MinIO S3, bỏ qua quá trình tải (Skipped)")
			return nil
		}
	}

	// 4. Tải stream zip archive từ Kaggle
	s.log.WithField("slug", s.cfg.DatasetSlug).Info("Bắt đầu tải dataset archive từ Kaggle...")
	kaggleCli := NewKaggleClient(s.cfg.KaggleUsername, s.cfg.KaggleKey)
	stream, contentLength, err := kaggleCli.DownloadDatasetStream(ctx, s.cfg.DatasetSlug)
	if err != nil {
		return fmt.Errorf("tải stream dataset từ Kaggle thất bại: %w", err)
	}
	defer stream.Close()

	// Wrap stream với ProgressReader để hiển thị phần trăm (%) và tốc độ tải trên Terminal
	progressStream := NewProgressReader(stream, contentLength, fmt.Sprintf("Kaggle Download (%s)", s.cfg.SelectedFile))

	// 5. Đọc stream vào bộ nhớ và tính toán SHA-256 Checksum đồng thời
	buf := new(bytes.Buffer)
	hasher := sha256.New()
	multiWriter := io.MultiWriter(buf, hasher)

	s.log.Info("Đang đọc dữ liệu archive và tính toán SHA-256 checksum...")
	if _, err := io.Copy(multiWriter, progressStream); err != nil {
		return fmt.Errorf("lỗi đọc dữ liệu stream từ Kaggle: %w", err)
	}

	zipBytes := buf.Bytes()
	archiveChecksum := hex.EncodeToString(hasher.Sum(nil))

	// 6. Upload Zip Archive lên MinIO S3
	s.log.WithField("key", archiveObjectKey).Info("Upload Zip Archive lên MinIO S3...")
	err = s.minioCli.UploadStream(ctx, archiveObjectKey, bytes.NewReader(zipBytes), int64(len(zipBytes)), "application/zip")
	if err != nil {
		return fmt.Errorf("upload Archive Zip lên MinIO thất bại: %w", err)
	}

	// 7. Giải nén an toàn file CSV từ Zip buffer (Zip Slip protected)
	s.log.WithField("target_file", s.cfg.SelectedFile).Info("Đang giải nén file CSV...")
	extractedFile, err := extractZipFileFromBuffer(zipBytes, s.cfg.SelectedFile)
	if err != nil {
		return fmt.Errorf("giải nén CSV thất bại: %w", err)
	}

	// 8. Upload file CSV lên MinIO S3
	s.log.WithField("key", extractedObjectKey).Info("Upload file CSV extracted lên MinIO S3...")
	err = s.minioCli.UploadStream(ctx, extractedObjectKey, extractedFile.Content, extractedFile.Size, "text/csv")
	if err != nil {
		return fmt.Errorf("upload CSV extracted lên MinIO thất bại: %w", err)
	}

	// 9. Ghi Dataset Manifest JSON lên MinIO S3
	datasetID := fmt.Sprintf("kaggle-%s", cleanSlug)
	manifest := DatasetManifest{
		DatasetID:       datasetID,
		DatasetSlug:     s.cfg.DatasetSlug,
		ArchivePath:     archiveObjectKey,
		ArchiveChecksum: archiveChecksum,
		ExtractedPath:   extractedObjectKey,
		SelectedFile:    s.cfg.SelectedFile,
		DownloadedAt:    time.Now().UTC(),
	}

	manifestBytes, err := json.MarshalIndent(manifest, "", "  ")
	if err != nil {
		return fmt.Errorf("serialize Dataset Manifest thất bại: %w", err)
	}

	err = s.minioCli.PutJSON(ctx, manifestObjectKey, manifestBytes)
	if err != nil {
		return fmt.Errorf("ghi Dataset Manifest lên MinIO thất bại: %w", err)
	}

	s.log.WithField("manifest_key", manifestObjectKey).Info("Đã đồng bộ Dataset thành công xuất sắc!")
	return nil
}

// extractZipFileFromBuffer giải nén file được chọn từ bộ nhớ zip buffer an toàn (Zip Slip Protection)
func extractZipFileFromBuffer(zipBytes []byte, targetFilename string) (*ExtractedFile, error) {
	zipReader, err := zip.NewReader(bytes.NewReader(zipBytes), int64(len(zipBytes)))
	if err != nil {
		return nil, fmt.Errorf("mở zip buffer thất bại: %w", err)
	}

	for _, file := range zipReader.File {
		cleanPath := filepath.Clean(file.Name)
		if strings.HasPrefix(cleanPath, "..") || filepath.IsAbs(cleanPath) {
			return nil, fmt.Errorf("phát hiện tấn công Zip Slip với file path: %s", file.Name)
		}

		if file.Name == targetFilename || filepath.Base(file.Name) == targetFilename {
			rc, err := file.Open()
			if err != nil {
				return nil, fmt.Errorf("mở file %s trong zip thất bại: %w", file.Name, err)
			}
			defer rc.Close()

			buf := new(bytes.Buffer)
			size, err := io.Copy(buf, rc)
			if err != nil {
				return nil, fmt.Errorf("đọc nội dung file %s thất bại: %w", file.Name, err)
			}

			return &ExtractedFile{
				Filename: targetFilename,
				Size:     size,
				Content:  bytes.NewReader(buf.Bytes()),
			}, nil
		}
	}

	return nil, fmt.Errorf("không tìm thấy file %s trong archive zip", targetFilename)
}
