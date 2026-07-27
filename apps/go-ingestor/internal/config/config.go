package config

import (
	"fmt"
	"os"
	"strings"
)

// Config chứa toàn bộ thông số cấu hình của Go Ingestor
type Config struct {
	KaggleUsername    string   // Username tài khoản Kaggle
	KaggleKey         string   // API Key xác thực Kaggle
	DatasetSlug       string   // Slug dataset trên Kaggle (vd: daniboy370/pubg-finish-placement-prediction)
	SelectedFile      string   // Tên file CSV chính cần trích xuất (vd: train_V2.csv)
	MinIOEndpoint     string   // Endpoint của MinIO S3 (vd: localhost:9000)
	MinIOAccessKey    string   // MinIO Access Key
	MinIOSecretKey    string   // MinIO Secret Key
	MinIOBucket       string   // Tên MinIO bucket chính (vd: pubg-data)
	MinIOUseSSL       bool     // Sử dụng SSL/TLS kết nối MinIO không
	ForceDownload     bool     // Bắt buộc tải lại dù dataset đã tồn tại
	KafkaBrokers      []string // Danh sách Kafka Brokers (Fail-Close nếu thiếu)
	KafkaRawTopic     string   // Topic Kafka phát dữ liệu chuẩn (pubg.v1.player-stat.raw)
	KafkaInvalidTopic string   // Topic Kafka phát dữ liệu lỗi (pubg.v1.invalid)
}

// LoadFromEnv nạp cấu hình từ Environment Variables (Áp dụng Fail-Close validation)
func LoadFromEnv() (*Config, error) {
	brokersStr := os.Getenv("KAFKA_BROKERS")
	var brokers []string
	if brokersStr != "" {
		for _, b := range strings.Split(brokersStr, ",") {
			trimmed := strings.TrimSpace(b)
			if trimmed != "" {
				brokers = append(brokers, trimmed)
			}
		}
	}

	cfg := &Config{
		KaggleUsername:    getEnv("KAGGLE_USERNAME", ""),
		KaggleKey:         getEnv("KAGGLE_KEY", ""),
		DatasetSlug:       getEnv("KAGGLE_DATASET_SLUG", "daniboy370/pubg-finish-placement-prediction"),
		SelectedFile:      getEnv("KAGGLE_SELECTED_FILE", "train_V2.csv"),
		MinIOEndpoint:     getEnv("MINIO_ENDPOINT", "localhost:9000"),
		MinIOAccessKey:    getEnv("MINIO_ACCESS_KEY", "minioadmin"),
		MinIOSecretKey:    getEnv("MINIO_SECRET_KEY", "minioadmin"),
		MinIOBucket:       getEnv("MINIO_BUCKET", "pubg-data"),
		MinIOUseSSL:       getEnv("MINIO_USE_SSL", "false") == "true",
		ForceDownload:     getEnv("FORCE_DOWNLOAD", "false") == "true",
		KafkaBrokers:      brokers,
		KafkaRawTopic:     os.Getenv("KAFKA_RAW_TOPIC"),     // Fail-Close: Không set default
		KafkaInvalidTopic: os.Getenv("KAFKA_INVALID_TOPIC"), // Fail-Close: Không set default
	}

	// Validate nghiêm ngặt: Kiểm tra Fail-Close nếu thiếu biến quan trọng
	if err := cfg.Validate(); err != nil {
		return nil, fmt.Errorf("cấu hình không hợp lệ (Fail-Close): %w", err)
	}

	return cfg, nil
}

// Validate kiểm tra tính hợp lệ của các thuộc tính bắt buộc (Fail-Fast)
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
	// Fail-Close Validation cho Kafka Configuration
	if len(c.KafkaBrokers) == 0 {
		return fmt.Errorf("KAFKA_BROKERS bắt buộc phải nạp từ môi trường (Fail-Close)")
	}
	if c.KafkaRawTopic == "" {
		return fmt.Errorf("KAFKA_RAW_TOPIC bắt buộc phải nạp từ môi trường (Fail-Close)")
	}
	if c.KafkaInvalidTopic == "" {
		return fmt.Errorf("KAFKA_INVALID_TOPIC bắt buộc phải nạp từ môi trường (Fail-Close)")
	}
	return nil
}

// getEnv đọc giá trị env var hoặc trả về fallback nếu rỗng
func getEnv(key, fallback string) string {
	val := os.Getenv(key)
	if val == "" {
		return fallback
	}
	return val
}
