package config

import (
	"fmt"
	"os"
)

// Config chứa toàn bộ thông số cấu hình của Go Ingestor
type Config struct {
	KaggleUsername   string // Username tài khoản Kaggle
	KaggleKey        string // API Key xác thực Kaggle
	DatasetSlug      string // Slug dataset trên Kaggle (vd: daniboy370/pubg-finish-placement-prediction)
	SelectedFile     string // Tên file CSV chính cần trích xuất (vd: train_V2.csv)
	MinIOEndpoint    string // Endpoint của MinIO S3 (vd: localhost:9000)
	MinIOAccessKey   string // MinIO Access Key
	MinIOSecretKey   string // MinIO Secret Key
	MinIOBucket      string // Tên MinIO bucket chính (vd: pubg-data)
	MinIOUseSSL      bool   // Sử dụng SSL/TLS kết nối MinIO không
	ForceDownload    bool   // Bắt buộc tải lại dù dataset đã tồn tại
}

// LoadFromEnv nạp cấu hình từ Environment Variables
func LoadFromEnv() (*Config, error) {
	cfg := &Config{
		KaggleUsername: getEnv("KAGGLE_USERNAME", ""),
		KaggleKey:      getEnv("KAGGLE_KEY", ""),
		DatasetSlug:    getEnv("KAGGLE_DATASET_SLUG", "daniboy370/pubg-finish-placement-prediction"),
		SelectedFile:   getEnv("KAGGLE_SELECTED_FILE", "train_V2.csv"),
		MinIOEndpoint:  getEnv("MINIO_ENDPOINT", "localhost:9000"),
		MinIOAccessKey: getEnv("MINIO_ACCESS_KEY", "minioadmin"),
		MinIOSecretKey: getEnv("MINIO_SECRET_KEY", "minioadmin"),
		MinIOBucket:    getEnv("MINIO_BUCKET", "pubg-data"),
		MinIOUseSSL:    getEnv("MINIO_USE_SSL", "false") == "true",
		ForceDownload:  getEnv("FORCE_DOWNLOAD", "false") == "true",
	}

	// Validate kiểm tra sự hợp lệ của cấu hình
	if err := cfg.Validate(); err != nil {
		return nil, fmt.Errorf("cấu hình không hợp lệ: %w", err)
	}

	return cfg, nil
}

// Validate kiểm tra tính hợp lệ của các thuộc tính bắt buộc
func (c *Config) Validate() error {
	if c.DatasetSlug == "" {
		return fmt.Errorf("KAGGLE_DATASET_SLUG không được để trống")
	}
	if c.MinIOEndpoint == "" {
		return fmt.Errorf("MINIO_ENDPOINT không được để trống")
	}
	if c.MinIOBucket == "" {
		return fmt.Errorf("MINIO_BUCKET không được để trống")
	}
	return nil
}

// getEnv đọc giá trị env var hoặc trả về fallback nếu rỗng
func getEnv(key, fallback string) string {
	if val, ok := os.LookupEnv(key); ok && val != "" {
		return val
	}
	return fallback
}
