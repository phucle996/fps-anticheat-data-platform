package app

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/hex"
	"fmt"
	"io"
	"strings"

	"github.com/sirupsen/logrus"

	"pubg-anti-cheat/go-ingestor/internal/config"
	"pubg-anti-cheat/go-ingestor/internal/dataset"
	"pubg-anti-cheat/go-ingestor/internal/source/kaggle"
	"pubg-anti-cheat/go-ingestor/internal/storage"
)

// DatasetSyncApp điều phối usecase tải và đồng bộ Dataset từ Kaggle lên MinIO S3 Data Lake
type DatasetSyncApp struct {
	cfg      *config.Config        // Cấu hình ứng dụng
	minioCli *storage.MinIOClient  // Client tương tác MinIO S3
	log      *logrus.Entry         // Logrus JSON Logger
}

// NewDatasetSyncApp khởi tạo ứng dụng DatasetSyncApp với đầy đủ dependencies
func NewDatasetSyncApp(cfg *config.Config, log *logrus.Entry) (*DatasetSyncApp, error) {
	// 1. Khởi tạo kết nối tới MinIO Object Storage
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

	return &DatasetSyncApp{
		cfg:      cfg,
		minioCli: minioCli,
		log:      log,
	}, nil
}

// Run thực thi quy trình đồng bộ dữ liệu end-to-end
func (a *DatasetSyncApp) Run(ctx context.Context) error {
	a.log.Info("Khởi động Use Case Dataset Sync...")

	// 1. Đảm bảo MinIO Bucket chính đã tồn tại
	if err := a.minioCli.EnsureBucketExists(ctx); err != nil {
		return fmt.Errorf("đảm bảo MinIO bucket tồn tại thất bại: %w", err)
	}

	// 2. Chuẩn bị Object Keys trên MinIO S3
	cleanSlug := strings.ReplaceAll(a.cfg.DatasetSlug, "/", "-")
	archiveObjectKey := fmt.Sprintf("archives/%s/dataset.zip", cleanSlug)
	extractedObjectKey := fmt.Sprintf("raw-sources/%s/%s", cleanSlug, a.cfg.SelectedFile)
	manifestObjectKey := "manifests/dataset-manifest.json"

	// 3. Kiểm tra nếu Dataset đã được đồng bộ sẵn trên MinIO
	if !a.cfg.ForceDownload {
		manifestExists, _ := a.minioCli.ObjectExists(ctx, manifestObjectKey)
		extractedExists, _ := a.minioCli.ObjectExists(ctx, extractedObjectKey)

		if manifestExists && extractedExists {
			a.log.WithFields(logrus.Fields{
				"manifest":      manifestObjectKey,
				"extracted_csv": extractedObjectKey,
			}).Info("Dataset đã tồn tại đầy đủ trên MinIO S3, bỏ qua quá trình tải (Skipped)")
			return nil
		}
	}

	// 4. Kiểm tra xem Context đã bị Hủy/Timeout chưa (Graceful Shutdown check)
	select {
	case <-ctx.Done():
		return ctx.Err()
	default:
	}

	// 5. Khởi tạo Kaggle Client và tải stream dữ liệu
	a.log.WithField("slug", a.cfg.DatasetSlug).Info("Bắt đầu tải dataset archive từ Kaggle...")
	kaggleCli := kaggle.NewClient(a.cfg.KaggleUsername, a.cfg.KaggleKey)
	stream, err := kaggleCli.DownloadDatasetStream(ctx, a.cfg.DatasetSlug)
	if err != nil {
		return fmt.Errorf("tải stream dataset từ Kaggle thất bại: %w", err)
	}
	defer stream.Close()

	// 6. Đọc stream dữ liệu và tính toán SHA-256 checksum đồng thời
	buf := new(bytes.Buffer)
	hasher := sha256.New()
	multiWriter := io.MultiWriter(buf, hasher)

	a.log.Info("Đang đọc dữ liệu archive và tính toán SHA-256 checksum...")
	if _, err := io.Copy(multiWriter, stream); err != nil {
		return fmt.Errorf("lỗi đọc dữ liệu stream từ Kaggle: %w", err)
	}

	zipBytes := buf.Bytes()
	archiveChecksum := hex.EncodeToString(hasher.Sum(nil))
	a.log.WithFields(logrus.Fields{
		"size_bytes": len(zipBytes),
		"sha256":     archiveChecksum,
	}).Info("Tải thành công archive zip")

	// 7. Upload Archive Zip lên MinIO S3
	a.log.WithField("key", archiveObjectKey).Info("Đang upload Archive Zip lên MinIO S3...")
	zipReader := bytes.NewReader(zipBytes)
	err = a.minioCli.UploadStream(ctx, archiveObjectKey, zipReader, int64(len(zipBytes)), "application/zip")
	if err != nil {
		return fmt.Errorf("upload Archive Zip lên MinIO thất bại: %w", err)
	}

	// 8. Giải nén Archive Zip để lấy file CSV chỉ định (Zip Slip protected)
	a.log.WithField("target_file", a.cfg.SelectedFile).Info("Đang trích xuất file CSV từ Archive Zip...")
	extractedFile, err := dataset.ExtractZipFileFromBuffer(zipBytes, a.cfg.SelectedFile)
	if err != nil {
		return fmt.Errorf("giải nén CSV từ Zip thất bại: %w", err)
	}

	// 9. Upload file CSV đã trích xuất lên MinIO S3
	a.log.WithField("key", extractedObjectKey).Info("Đang upload file CSV extracted lên MinIO S3...")
	err = a.minioCli.UploadStream(ctx, extractedObjectKey, extractedFile.Content, extractedFile.Size, "text/csv")
	if err != nil {
		return fmt.Errorf("upload file CSV extracted lên MinIO thất bại: %w", err)
	}

	// 10. Tạo và Ghi Dataset Manifest lên MinIO S3
	a.log.Info("Đang khởi tạo Dataset Manifest...")
	datasetID := fmt.Sprintf("kaggle-%s", cleanSlug)
	manifest := dataset.NewDatasetManifest(
		datasetID,
		a.cfg.DatasetSlug,
		archiveObjectKey,
		archiveChecksum,
		extractedObjectKey,
		a.cfg.SelectedFile,
	)

	manifestBytes, err := manifest.ToJSON()
	if err != nil {
		return fmt.Errorf("serialize Dataset Manifest thất bại: %w", err)
	}

	err = a.minioCli.PutJSON(ctx, manifestObjectKey, manifestBytes)
	if err != nil {
		return fmt.Errorf("ghi Dataset Manifest lên MinIO thất bại: %w", err)
	}

	a.log.WithField("manifest_key", manifestObjectKey).Info("Đã đồng bộ thành công Dataset Manifest lên MinIO S3")
	return nil
}
