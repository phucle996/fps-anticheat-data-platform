package app

import (
	"archive/zip"
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"strings"
	"time"

	"github.com/sirupsen/logrus"

	"go-ingestor/internal/config"
	"go-ingestor/internal/pipeline"
	"go-ingestor/internal/provider"
	"go-ingestor/internal/storage"
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

// DatasetSyncService điều phối usecase tải và đồng bộ Dataset từ Kaggle lên MinIO S3
type DatasetSyncService struct {
	cfg      *config.Config
	minioCli *storage.MinIOClient
	log      *logrus.Entry
}

// NewDatasetSyncService khởi tạo DatasetSyncService
func NewDatasetSyncService(cfg *config.Config, log *logrus.Entry) (*DatasetSyncService, error) {
	minioCli, err := storage.NewMinIOClient(
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

// Run thực thi usecase đồng bộ dataset với cơ chế Cloud-Native Zero-RAM Streaming & Disk Spooling
func (s *DatasetSyncService) Run(ctx context.Context) error {
	s.log.Info("Khởi động Use Case Dataset Sync (Zero-RAM Disk Spooling & Direct MinIO Streaming Mode)...")

	// 1. Đảm bảo bucket chính đã tồn tại trên MinIO S3
	if err := s.minioCli.EnsureBucketExists(ctx); err != nil {
		return fmt.Errorf("đảm bảo MinIO bucket tồn tại thất bại: %w", err)
	}

	// 2. Định nghĩa các Object Keys cơ sở trên MinIO S3
	cleanSlug := strings.ReplaceAll(s.cfg.DatasetSlug, "/", "-")
	archiveObjectKey := fmt.Sprintf("archives/%s/dataset.zip", cleanSlug)
	manifestObjectKey := "manifests/dataset-manifest.json"

	// 3. Tạo file đĩa tạm trong /tmp để chứa zip archive (Tránh nạp toàn bộ archive 4.2GB vào RAM)
	tmpFile, err := os.CreateTemp("", "pubg_dataset_archive_*.zip")
	if err != nil {
		return fmt.Errorf("không thể tạo file đĩa tạm trong /tmp: %w", err)
	}
	tmpFilePath := tmpFile.Name()
	// Đảm bảo luôn thu hồi dọn dẹp file đĩa tạm khi hàm kết thúc hoặc dừng đột ngột (Graceful Cleanup)
	defer os.Remove(tmpFilePath)
	defer tmpFile.Close()

	var archiveChecksum string
	archiveExists, _ := s.minioCli.ObjectExists(ctx, archiveObjectKey)

	if archiveExists && !s.cfg.ForceDownload {
		// Tái sử dụng zip archive đã có trên MinIO S3: Stream trực tiếp từ MinIO xuống đĩa tạm
		s.log.WithField("key", archiveObjectKey).Info("Tái sử dụng archive zip từ MinIO S3, ghi stream xuống đĩa tạm...")
		stream, err := s.minioCli.DownloadStream(ctx, archiveObjectKey)
		if err != nil {
			return fmt.Errorf("tải archive từ MinIO thất bại: %w", err)
		}
		defer stream.Close()

		hasher := sha256.New()
		// Ghi stream trực tiếp xuống file tạm đồng thời tính toán SHA-256 Checksum
		multiWriter := io.MultiWriter(tmpFile, hasher)
		if _, err := io.Copy(multiWriter, stream); err != nil {
			return fmt.Errorf("lỗi đọc archive từ MinIO xuống đĩa tạm: %w", err)
		}
		archiveChecksum = hex.EncodeToString(hasher.Sum(nil))
	} else {
		// Tải stream zip archive trực tiếp từ Kaggle API và ghi thẳng xuống đĩa tạm
		s.log.WithField("slug", s.cfg.DatasetSlug).Info("Bắt đầu tải dataset archive từ Kaggle API...")
		kaggleCli := provider.NewKaggleClient(s.cfg.KaggleUsername, s.cfg.KaggleKey)
		stream, contentLength, err := kaggleCli.DownloadDatasetStream(ctx, s.cfg.DatasetSlug)
		if err != nil {
			return fmt.Errorf("tải stream dataset từ Kaggle thất bại: %w", err)
		}
		defer stream.Close()

		// Wrap stream với ProgressReader để hiển thị phần trăm (%) và tốc độ tải trên Terminal
		progressStream := pipeline.NewProgressReader(stream, contentLength, fmt.Sprintf("Kaggle Download (%s)", s.cfg.SelectedFile))
		hasher := sha256.New()

		// Stream trực tiếp từ Kaggle API xuống đĩa tạm đồng thời tính toán SHA-256 Checksum
		multiWriter := io.MultiWriter(tmpFile, hasher)
		s.log.Info("Đang ghi dữ liệu archive xuống đĩa tạm và tính toán SHA-256 checksum...")
		if _, err := io.Copy(multiWriter, progressStream); err != nil {
			return fmt.Errorf("lỗi ghi dữ liệu stream từ Kaggle xuống đĩa tạm: %w", err)
		}
		archiveChecksum = hex.EncodeToString(hasher.Sum(nil))

		// Seek file đĩa tạm về vị trí bắt đầu (offset 0) để chuẩn bị upload lên MinIO S3
		if _, err := tmpFile.Seek(0, io.SeekStart); err != nil {
			return fmt.Errorf("seek file đĩa tạm về offset 0 thất bại: %w", err)
		}

		fi, err := tmpFile.Stat()
		if err != nil {
			return fmt.Errorf("lấy thông tin file đĩa tạm thất bại: %w", err)
		}

		// Upload Zip Archive gốc từ đĩa tạm lên MinIO S3
		s.log.WithField("key", archiveObjectKey).Info("Upload Zip Archive gốc từ đĩa tạm lên MinIO S3...")
		err = s.minioCli.UploadStream(ctx, archiveObjectKey, tmpFile, fi.Size(), "application/zip")
		if err != nil {
			return fmt.Errorf("upload Archive Zip lên MinIO thất bại: %w", err)
		}
	}

	// 4. Mở Zip archive trực tiếp từ đĩa tạm với Random Access Index (Constant O(1) RAM)
	s.log.Info("Đang giải nén và stream trực tiếp tất cả các file CSV từ đĩa tạm lên MinIO S3...")
	zipReader, err := zip.OpenReader(tmpFilePath)
	if err != nil {
		return fmt.Errorf("mở file zip từ đĩa tạm thất bại: %w", err)
	}
	defer zipReader.Close()

	var extractedKeys []string
	var defaultExtractedKey string

	// Duyệt và stream từng file CSV lên MinIO S3
	for _, file := range zipReader.File {
		// Kiểm tra an toàn Zip Slip: ngăn chặn các đường dẫn nguy hiểm như ../../etc/passwd
		cleanPath := filepath.Clean(file.Name)
		if strings.HasPrefix(cleanPath, "..") || filepath.IsAbs(cleanPath) {
			return fmt.Errorf("phát hiện tấn công Zip Slip với file path: %s", file.Name)
		}

		// Bỏ qua thư mục
		if file.FileInfo().IsDir() {
			continue
		}

		// Chỉ lọc giải nén các file có phần mở rộng .csv (không phân biệt hoa thường)
		if strings.HasSuffix(strings.ToLower(file.Name), ".csv") {
			filename := filepath.Base(file.Name)
			extractedObjectKey := fmt.Sprintf("raw-sources/%s/%s", cleanSlug, filename)
			extractedKeys = append(extractedKeys, extractedObjectKey)

			// Gán file mặc định nếu khớp với tên SelectedFile cấu hình
			if filename == s.cfg.SelectedFile || filepath.Base(filename) == s.cfg.SelectedFile {
				defaultExtractedKey = extractedObjectKey
			}

			// Kiểm tra xem file CSV này đã tồn tại trên MinIO S3 chưa (Idempotency & Race Condition Guard)
			csvExists, _ := s.minioCli.ObjectExists(ctx, extractedObjectKey)
			if csvExists && !s.cfg.ForceDownload {
				s.log.WithField("key", extractedObjectKey).Info("File CSV đã tồn tại trên MinIO S3, bỏ qua upload (Skipped)")
				continue
			}

			s.log.WithFields(logrus.Fields{
				"key":               extractedObjectKey,
				"uncompressed_size": file.UncompressedSize64,
			}).Info("Stream giải nén trực tiếp file CSV từ đĩa tạm lên MinIO S3...")

			// Mở luồng stream giải nén cho từng file CSV đơn lẻ
			rc, err := file.Open()
			if err != nil {
				return fmt.Errorf("mở file %s trong zip archive thất bại: %w", file.Name, err)
			}

			// Stream trực tiếp từ reader của zip entry lên MinIO S3 (Không lưu mảng byte CSV vào RAM)
			err = s.minioCli.UploadStream(ctx, extractedObjectKey, rc, int64(file.UncompressedSize64), "text/csv")
			rc.Close() // Giải phóng luồng đọc ngay lập tức sau khi stream xong từng file
			if err != nil {
				return fmt.Errorf("upload CSV extracted '%s' lên MinIO thất bại: %w", extractedObjectKey, err)
			}
		}
	}

	if len(extractedKeys) == 0 {
		return fmt.Errorf("không tìm thấy file CSV nào trong archive zip")
	}

	// Nếu không tìm thấy match chính xác với SelectedFile, gán file đầu tiên làm mặc định
	if defaultExtractedKey == "" && len(extractedKeys) > 0 {
		defaultExtractedKey = extractedKeys[0]
	}

	// 5. Ghi Dataset Manifest JSON tổng hợp lên MinIO S3
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
		"total_csvs":   len(extractedKeys),
	}).Info("Đã đồng bộ trọn bộ Dataset lên MinIO S3 thành công xuất sắc!")
	return nil
}
