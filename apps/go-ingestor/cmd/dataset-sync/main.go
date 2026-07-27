package main

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/hex"
	"fmt"
	"io"
	"log/slog"
	"os"
	"strings"

	"pubg-anti-cheat/go-ingestor/internal/config"
	"pubg-anti-cheat/go-ingestor/internal/dataset"
	"pubg-anti-cheat/go-ingestor/internal/logging"
	"pubg-anti-cheat/go-ingestor/internal/source/kaggle"
	"pubg-anti-cheat/go-ingestor/internal/storage"
)

func main() {
	// 1. Khởi tạo JSON Logger cho dịch vụ
	logger := logging.InitLogger("go-ingestor")
	slog.Info("Khởi động tiến trình dataset-sync...")

	// 2. Nạp cấu hình ứng dụng từ Environment Variables
	cfg, err := config.LoadFromEnv()
	if err != nil {
		slog.Error("Nạp cấu hình thất bại", "error", err)
		os.Exit(1)
	}

	// 3. Khởi tạo context với Timeout cho toàn bộ tiến trình
	ctx := context.Background()

	// 4. Khởi tạo kết nối tới MinIO Object Storage
	minioCli, err := storage.NewMinIOClient(
		cfg.MinIOEndpoint,
		cfg.MinIOAccessKey,
		cfg.MinIOSecretKey,
		cfg.MinIOBucket,
		cfg.MinIOUseSSL,
	)
	if err != nil {
		slog.Error("Khởi tạo kết nối MinIO thất bại", "error", err)
		os.Exit(1)
	}

	// Đảm bảo Bucket MinIO đã tồn tại
	if err := minioCli.EnsureBucketExists(ctx); err != nil {
		slog.Error("Đảm bảo MinIO bucket tồn tại thất bại", "error", err)
		os.Exit(1)
	}

	// 5. Chuẩn bị Object Keys trên MinIO S3
	cleanSlug := strings.ReplaceAll(cfg.DatasetSlug, "/", "-")
	archiveObjectKey := fmt.Sprintf("archives/%s/dataset.zip", cleanSlug)
	extractedObjectKey := fmt.Sprintf("raw-sources/%s/%s", cleanSlug, cfg.SelectedFile)
	manifestObjectKey := "manifests/dataset-manifest.json"

	// 6. Kiểm tra xem Dataset đã được đồng bộ sẵn lên MinIO chưa
	if !cfg.ForceDownload {
		manifestExists, _ := minioCli.ObjectExists(ctx, manifestObjectKey)
		extractedExists, _ := minioCli.ObjectExists(ctx, extractedObjectKey)

		if manifestExists && extractedExists {
			slog.Info("Dataset đã tồn tại đầy đủ trên MinIO S3, bỏ qua quá trình tải",
				"manifest", manifestObjectKey,
				"extracted_csv", extractedObjectKey,
			)
			slog.Info("Tiến trình dataset-sync hoàn tất thành công (Skipped).")
			os.Exit(0)
		}
	}

	// 7. Khởi tạo Kaggle API Client và tải Stream dữ liệu
	slog.Info("Bắt đầu tải dataset archive từ Kaggle...", "slug", cfg.DatasetSlug)
	kaggleCli := kaggle.NewClient(cfg.KaggleUsername, cfg.KaggleKey)
	stream, err := kaggleCli.DownloadDatasetStream(ctx, cfg.DatasetSlug)
	if err != nil {
		slog.Error("Tải stream dataset từ Kaggle thất bại", "error", err)
		os.Exit(1)
	}
	defer stream.Close()

	// 8. Đọc dữ liệu vào memory buffer (hoặc temp file) đồng thời tính toán SHA-256 Checksum
	buf := new(bytes.Buffer)
	hasher := sha256.New()
	multiWriter := io.MultiWriter(buf, hasher)

	slog.Info("Đang đọc dữ liệu archive và tính toán SHA-256 checksum...")
	if _, err := io.Copy(multiWriter, stream); err != nil {
		slog.Error("Lỗi đọc dữ liệu stream từ Kaggle", "error", err)
		os.Exit(1)
	}

	zipBytes := buf.Bytes()
	archiveChecksum := hex.EncodeToString(hasher.Sum(nil))
	slog.Info("Tải thành công archive zip",
		"size_bytes", len(zipBytes),
		"sha256", archiveChecksum,
	)

	// 9. Upload Archive Zip lên MinIO S3
	slog.Info("Đang upload Archive Zip lên MinIO S3...", "key", archiveObjectKey)
	zipReader := bytes.NewReader(zipBytes)
	err = minioCli.UploadStream(ctx, archiveObjectKey, zipReader, int64(len(zipBytes)), "application/zip")
	if err != nil {
		slog.Error("Upload Archive Zip lên MinIO thất bại", "error", err)
		os.Exit(1)
	}

	// 10. Giải nén Archive Zip để lấy file CSV chỉ định (ví dụ: train_V2.csv)
	slog.Info("Đang trích xuất file CSV từ Archive Zip...", "target_file", cfg.SelectedFile)
	extractedFile, err := dataset.ExtractZipFileFromBuffer(zipBytes, cfg.SelectedFile)
	if err != nil {
		slog.Error("Giải nén CSV từ Zip thất bại", "error", err)
		os.Exit(1)
	}

	// 11. Upload file CSV đã trích xuất lên MinIO S3
	slog.Info("Đang upload file CSV extracted lên MinIO S3...", "key", extractedObjectKey)
	err = minioCli.UploadStream(ctx, extractedObjectKey, extractedFile.Content, extractedFile.Size, "text/csv")
	if err != nil {
		slog.Error("Upload file CSV extracted lên MinIO thất bại", "error", err)
		os.Exit(1)
	}

	// 12. Tạo và Ghi Dataset Manifest lên MinIO S3
	slog.Info("Đang tạo Dataset Manifest...")
	datasetID := fmt.Sprintf("kaggle-%s", cleanSlug)
	manifest := dataset.NewDatasetManifest(
		datasetID,
		cfg.DatasetSlug,
		archiveObjectKey,
		archiveChecksum,
		extractedObjectKey,
		cfg.SelectedFile,
	)

	manifestBytes, err := manifest.ToJSON()
	if err != nil {
		slog.Error("Serialize Dataset Manifest thất bại", "error", err)
		os.Exit(1)
	}

	err = minioCli.PutJSON(ctx, manifestObjectKey, manifestBytes)
	if err != nil {
		slog.Error("Ghi Dataset Manifest lên MinIO thất bại", "error", err)
		os.Exit(1)
	}

	slog.Info("Đã ghi thành công Dataset Manifest lên MinIO", "manifest_key", manifestObjectKey)
	slog.Info("Tiến trình dataset-sync hoàn thành xuất sắc!")
}
