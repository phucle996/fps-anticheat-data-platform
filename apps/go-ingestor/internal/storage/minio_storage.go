package storage

import (
	"bytes"
	"context"
	"fmt"
	"io"
	"log/slog"

	"github.com/minio/minio-go/v7"
	"github.com/minio/minio-go/v7/pkg/credentials"
)

// MinIOClient bọc SDK Client của MinIO để thao tác với Data Lake S3
type MinIOClient struct {
	client *minio.Client // S3 SDK Client
	bucket string        // Tên bucket mặc định
}

// NewMinIOClient khởi tạo kết nối S3 Client tới MinIO
func NewMinIOClient(endpoint, accessKey, secretKey, bucket string, useSSL bool) (*MinIOClient, error) {
	// Khởi tạo credentials cho MinIO
	creds := credentials.NewStaticV4(accessKey, secretKey, "")

	// Kết nối Client SDK
	cli, err := minio.New(endpoint, &minio.Options{
		Creds:  creds,
		Secure: useSSL,
	})
	if err != nil {
		return nil, fmt.Errorf("không thể kết nối MinIO Client: %w", err)
	}

	return &MinIOClient{
		client: cli,
		bucket: bucket,
	}, nil
}

// EnsureBucketExists kiểm tra và tự động tạo Bucket nếu chưa có
func (m *MinIOClient) EnsureBucketExists(ctx context.Context) error {
	exists, err := m.client.BucketExists(ctx, m.bucket)
	if err != nil {
		return fmt.Errorf("lỗi kiểm tra bucket tồn tại: %w", err)
	}
	if !exists {
		slog.Info("Bucket chưa tồn tại, đang tiến hành tạo mới", "bucket", m.bucket)
		err = m.client.MakeBucket(ctx, m.bucket, minio.MakeBucketOptions{})
		if err != nil {
			return fmt.Errorf("không thể tạo bucket: %w", err)
		}
	}
	return nil
}

// ObjectExists kiểm tra xem một object key đã có trên MinIO hay chưa
func (m *MinIOClient) ObjectExists(ctx context.Context, objectKey string) (bool, error) {
	_, err := m.client.StatObject(ctx, m.bucket, objectKey, minio.StatObjectOptions{})
	if err != nil {
		errResponse := minio.ToErrorResponse(err)
		if errResponse.Code == "NoSuchKey" {
			return false, nil // File không tồn tại
		}
		return false, err // Lỗi khác
	}
	return true, nil // File đã tồn tại
}

// UploadStream đẩy một luồng dữ liệu (io.Reader) lên MinIO Object Storage
func (m *MinIOClient) UploadStream(ctx context.Context, objectKey string, reader io.Reader, size int64, contentType string) error {
	_, err := m.client.PutObject(ctx, m.bucket, objectKey, reader, size, minio.PutObjectOptions{
		ContentType: contentType,
	})
	if err != nil {
		return fmt.Errorf("lỗi upload object '%s' lên MinIO: %w", objectKey, err)
	}
	return nil
}

// PutJSON Ghi nội dung JSON bytes trực tiếp lên MinIO
func (m *MinIOClient) PutJSON(ctx context.Context, objectKey string, jsonBytes []byte) error {
	reader := bytes.NewReader(jsonBytes)
	return m.UploadStream(ctx, objectKey, reader, int64(len(jsonBytes)), "application/json")
}

// DownloadStream mở luồng đọc dữ liệu từ MinIO Object Storage
func (m *MinIOClient) DownloadStream(ctx context.Context, objectKey string) (*minio.Object, error) {
	obj, err := m.client.GetObject(ctx, m.bucket, objectKey, minio.GetObjectOptions{})
	if err != nil {
		return nil, fmt.Errorf("không thể tải object '%s' từ MinIO: %w", objectKey, err)
	}
	return obj, nil
}
