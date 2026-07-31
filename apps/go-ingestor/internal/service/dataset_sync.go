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
	DatasetID       string    `json:"dataset_id"`       // ID duy nhất (vd: kaggle-skihikingkevin-pubg-match-deaths)
	DatasetSlug     string    `json:"dataset_slug"`     // Slug Kaggle (skihikingkevin/pubg-match-deaths)
	ArchivePath     string    `json:"archive_path"`     // Key MinIO của file zip archive
	ArchiveChecksum string    `json:"archive_checksum"` // SHA-256 Checksum của file zip
	ExtractedPath   string    `json:"extracted_path"`   // Key MinIO của file CSV mặc định đã giải nén
	ExtractedPaths  []string  `json:"extracted_paths"`  // Danh sách toàn bộ các Object Keys CSV đã giải nén trên MinIO S3
	SelectedFile    string    `json:"selected_file"`    // Tên file CSV được chọn mặc định (kill_match_stats_final_0.csv)
	DownloadedAt    time.Time `json:"downloaded_at"`    // Thời điểm đồng bộ (UTC)
}

// ExtractedFile lưu thông tin file CSV đã giải nén từ bộ nhớ
type ExtractedFile struct {
	Filename string
	Size     int64
	Content  []byte
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
	s.log.Info("Khởi động Use Case Dataset Sync (Flat Architecture — Full Materialization Mode)...")

	// 1. Đảm bảo bucket chính đã tồn tại trên MinIO S3
	if err := s.minioCli.EnsureBucketExists(ctx); err != nil {
		return fmt.Errorf("đảm bảo MinIO bucket tồn tại thất bại: %w", err)
	}

	// 2. Định nghĩa các Object Keys cơ sở trên MinIO S3
	cleanSlug := strings.ReplaceAll(s.cfg.DatasetSlug, "/", "-")
	archiveObjectKey := fmt.Sprintf("archives/%s/dataset.zip", cleanSlug)
	manifestObjectKey := "manifests/dataset-manifest.json"

	// 3. Tải hoặc tái sử dụng archive `.zip` đã có trên MinIO
	var zipBytes []byte
	var archiveChecksum string
	archiveExists, _ := s.minioCli.ObjectExists(ctx, archiveObjectKey)
	if archiveExists && !s.cfg.ForceDownload {
		s.log.WithField("key", archiveObjectKey).Info("Tái sử dụng archive zip đã có sẵn trên MinIO S3 để giải nén toàn bộ dataset")
		stream, err := s.minioCli.DownloadStream(ctx, archiveObjectKey)
		if err != nil {
			return fmt.Errorf("tải archive từ MinIO thất bại: %w", err)
		}
		defer stream.Close()

		buf := new(bytes.Buffer)
		hasher := sha256.New()
		multiWriter := io.MultiWriter(buf, hasher)
		if _, err := io.Copy(multiWriter, stream); err != nil {
			return fmt.Errorf("lỗi đọc archive từ MinIO: %w", err)
		}

		zipBytes = buf.Bytes()
		archiveChecksum = hex.EncodeToString(hasher.Sum(nil))
	} else {
		// 4. Tải stream zip archive trực tiếp từ Kaggle API
		s.log.WithField("slug", s.cfg.DatasetSlug).Info("Bắt đầu tải dataset archive từ Kaggle API...")
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

		zipBytes = buf.Bytes()
		archiveChecksum = hex.EncodeToString(hasher.Sum(nil))

		// 6. Upload Zip Archive gốc lên MinIO S3
		s.log.WithField("key", archiveObjectKey).Info("Upload Zip Archive lên MinIO S3...")
		err = s.minioCli.UploadStream(ctx, archiveObjectKey, bytes.NewReader(zipBytes), int64(len(zipBytes)), "application/zip")
		if err != nil {
			return fmt.Errorf("upload Archive Zip lên MinIO thất bại: %w", err)
		}
	}

	// 7. Giải nén an toàn TOÀN BỘ các file CSV có trong Zip buffer (Zip Slip protected)
	s.log.Info("Đang giải nén trọn bộ tất cả các file CSV từ Zip archive...")
	extractedFiles, err := extractAllZipCSVFilesFromBuffer(zipBytes)
	if err != nil {
		return fmt.Errorf("giải nén trọn bộ CSV thất bại: %w", err)
	}

	// 8. Duyệt và upload từng file CSV lên MinIO S3 nếu chưa tồn tại
	var extractedKeys []string
	var defaultExtractedKey string

	for _, file := range extractedFiles {
		extractedObjectKey := fmt.Sprintf("raw-sources/%s/%s", cleanSlug, file.Filename)
		extractedKeys = append(extractedKeys, extractedObjectKey)

		// Lưu làm default extracted key nếu trùng khớp SelectedFile
		if file.Filename == s.cfg.SelectedFile || filepath.Base(file.Filename) == s.cfg.SelectedFile {
			defaultExtractedKey = extractedObjectKey
		}

		// Kiểm tra xem file CSV này đã tồn tại trên MinIO S3 chưa
		csvExists, _ := s.minioCli.ObjectExists(ctx, extractedObjectKey)
		if csvExists && !s.cfg.ForceDownload {
			s.log.WithField("key", extractedObjectKey).Info("File CSV đã tồn tại trên MinIO S3, bỏ qua upload (Skipped)")
			continue
		}

		// Upload file CSV lên MinIO S3
		s.log.WithFields(logrus.Fields{
			"key":  extractedObjectKey,
			"size": file.Size,
		}).Info("Upload file CSV extracted lên MinIO S3...")

		err = s.minioCli.UploadStream(ctx, extractedObjectKey, bytes.NewReader(file.Content), file.Size, "text/csv")
		if err != nil {
			return fmt.Errorf("upload CSV extracted '%s' lên MinIO thất bại: %w", extractedObjectKey, err)
		}
	}

	// Nếu không tìm thấy match chính xác với SelectedFile, gán file đầu tiên làm mặc định
	if defaultExtractedKey == "" && len(extractedKeys) > 0 {
		defaultExtractedKey = extractedKeys[0]
	}

	// 9. Ghi Dataset Manifest JSON tổng hợp lên MinIO S3
	datasetID := fmt.Sprintf("kaggle-%s", cleanSlug)
	manifest := DatasetManifest{
		DatasetID:       datasetID,
		DatasetSlug:     s.cfg.DatasetSlug,
		ArchivePath:     archiveObjectKey,
		ArchiveChecksum: archiveChecksum,
		ExtractedPath:   defaultExtractedKey,
		ExtractedPaths:  extractedKeys,
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

	s.log.WithFields(logrus.Fields{
		"manifest_key": manifestObjectKey,
		"total_csvs":   len(extractedFiles),
	}).Info("Đã đồng bộ trọn bộ Dataset lên MinIO S3 thành công xuất sắc!")
	return nil
}

// extractAllZipCSVFilesFromBuffer giải nén tất cả các file .csv từ bộ nhớ zip buffer an toàn (Zip Slip Protection)
func extractAllZipCSVFilesFromBuffer(zipBytes []byte) ([]*ExtractedFile, error) {
	zipReader, err := zip.NewReader(bytes.NewReader(zipBytes), int64(len(zipBytes)))
	if err != nil {
		return nil, fmt.Errorf("mở zip buffer thất bại: %w", err)
	}

	var extractedFiles []*ExtractedFile
	for _, file := range zipReader.File {
		// Kiểm tra an toàn Zip Slip: ngăn chặn các đường dẫn nguy hiểm như ../../etc/passwd
		cleanPath := filepath.Clean(file.Name)
		if strings.HasPrefix(cleanPath, "..") || filepath.IsAbs(cleanPath) {
			return nil, fmt.Errorf("phát hiện tấn công Zip Slip với file path: %s", file.Name)
		}

		// Bỏ qua thư mục
		if file.FileInfo().IsDir() {
			continue
		}

		// Chỉ chọn giải nén các file có phần mở rộng .csv (không phân biệt hoa thường)
		if strings.HasSuffix(strings.ToLower(file.Name), ".csv") {
			rc, err := file.Open()
			if err != nil {
				return nil, fmt.Errorf("mở file %s trong zip thất bại: %w", file.Name, err)
			}

			buf := new(bytes.Buffer)
			size, err := io.Copy(buf, rc)
			rc.Close()
			if err != nil {
				return nil, fmt.Errorf("đọc nội dung file %s thất bại: %w", file.Name, err)
			}

			// Lấy basename (ví dụ: kill_match_stats_final_0.csv) làm tên đại diện chuẩn hóa
			filename := filepath.Base(file.Name)
			extractedFiles = append(extractedFiles, &ExtractedFile{
				Filename: filename,
				Size:     size,
				Content:  buf.Bytes(),
			})
		}
	}

	if len(extractedFiles) == 0 {
		return nil, fmt.Errorf("không tìm thấy file CSV nào trong archive zip")
	}

	return extractedFiles, nil
}
