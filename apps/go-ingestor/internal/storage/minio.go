package storage

import (
	"bytes"
	"context"
	"fmt"
	"io"

	"github.com/minio/minio-go/v7"
	"github.com/minio/minio-go/v7/pkg/credentials"
)

// MinIOClient bọc client chính thức của MinIO S3 SDK
// Phục vụ việc lưu trữ dataset raw archive, extracted CSV, và checkpoint states
type MinIOClient struct {
	client     *minio.Client // Core MinIO Go Client
	bucketName string        // Tên bucket chính (vd: fps-anticheat-datalake)
}

// NewMinIOClient khởi tạo kết nối tới MinIO Object Storage Server với thông số xác thực
func NewMinIOClient(endpoint, accessKey, secretKey, bucketName string, useSSL bool) (*MinIOClient, error) {
	cli, err := minio.New(endpoint, &minio.Options{
		Creds:  credentials.NewStaticV4(accessKey, secretKey, ""),
		Secure: useSSL,
	})
	if err != nil {
		return nil, fmt.Errorf("khởi tạo MinIO client thất bại: %w", err)
	}

	return &MinIOClient{
		client:     cli,
		bucketName: bucketName,
	}, nil
}

// EnsureBucketExists kiểm tra và tự động tạo bucket nếu chưa tồn tại trên MinIO Server
func (m *MinIOClient) EnsureBucketExists(ctx context.Context) error {
	exists, err := m.client.BucketExists(ctx, m.bucketName)
	if err != nil {
		return fmt.Errorf("kiểm tra bucket %s thất bại: %w", m.bucketName, err)
	}

	if !exists {
		err = m.client.MakeBucket(ctx, m.bucketName, minio.MakeBucketOptions{})
		if err != nil {
			return fmt.Errorf("tạo bucket %s thất bại: %w", m.bucketName, err)
		}
	}
	return nil
}

// ObjectExists kiểm tra xem một object key đã tồn tại trên MinIO S3 chưa (Idempotency Check)
func (m *MinIOClient) ObjectExists(ctx context.Context, objectKey string) (bool, error) {
	_, err := m.client.StatObject(ctx, m.bucketName, objectKey, minio.StatObjectOptions{})
	if err != nil {
		errResponse := minio.ToErrorResponse(err)
		if errResponse.Code == "NoSuchKey" || errResponse.Code == "NotFound" {
			return false, nil
		}
		return false, err
	}
	return true, nil
}

// UploadStream đẩy luồng dữ liệu io.Reader lên MinIO S3 (Zero-RAM Streaming)
func (m *MinIOClient) UploadStream(ctx context.Context, objectKey string, reader io.Reader, objectSize int64, contentType string) error {
	_, err := m.client.PutObject(ctx, m.bucketName, objectKey, reader, objectSize, minio.PutObjectOptions{
		ContentType: contentType,
	})
	if err != nil {
		return fmt.Errorf("upload stream lên %s thất bại: %w", objectKey, err)
	}
	return nil
}

// PutJSON lưu dữ liệu byte JSON lên MinIO S3
func (m *MinIOClient) PutJSON(ctx context.Context, objectKey string, data []byte) error {
	reader := bytes.NewReader(data)
	return m.UploadStream(ctx, objectKey, reader, int64(len(data)), "application/json")
}

// DownloadStream mở luồng stream đọc object từ MinIO S3
func (m *MinIOClient) DownloadStream(ctx context.Context, objectKey string) (io.ReadCloser, error) {
	obj, err := m.client.GetObject(ctx, m.bucketName, objectKey, minio.GetObjectOptions{})
	if err != nil {
		return nil, fmt.Errorf("tải object %s từ MinIO thất bại: %w", objectKey, err)
	}
	return obj, nil
}

// RemoveObject xóa vật lý một object key khỏi MinIO S3
func (m *MinIOClient) RemoveObject(ctx context.Context, objectKey string) error {
	err := m.client.RemoveObject(ctx, m.bucketName, objectKey, minio.RemoveObjectOptions{})
	if err != nil {
		return fmt.Errorf("xóa object %s trên MinIO thất bại: %w", objectKey, err)
	}
	return nil
}
