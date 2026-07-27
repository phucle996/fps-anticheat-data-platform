package config

import (
	"fmt"
	"os"
	"strings"
)

// Config chứa toàn bộ thông số cấu hình của Go Ingestor (Áp dụng nghiêm ngặt nguyên tắc Fail-Close 100%)
type Config struct {
	KaggleUsername    string   // Username tài khoản Kaggle (Fail-Close nếu rỗng khi sync)
	KaggleKey         string   // API Key xác thực Kaggle (Fail-Close nếu rỗng khi sync)
	DatasetSlug       string   // Slug dataset trên Kaggle (Bắt buộc KAGGLE_DATASET_SLUG)
	SelectedFile      string   // Tên file CSV chính cần trích xuất (Bắt buộc KAGGLE_SELECTED_FILE)
	MinIOEndpoint     string   // Endpoint của MinIO S3 (Bắt buộc MINIO_ENDPOINT)
	MinIOAccessKey    string   // MinIO Access Key (Bắt buộc MINIO_ACCESS_KEY)
	MinIOSecretKey    string   // MinIO Secret Key (Bắt buộc MINIO_SECRET_KEY)
	MinIOBucket       string   // Tên MinIO bucket chính (Bắt buộc MINIO_BUCKET)
	MinIOUseSSL       bool     // Sử dụng SSL/TLS kết nối MinIO không
	ForceDownload     bool     // Bắt buộc tải lại dataset dù đã tồn tại
	KafkaBrokers      []string // Danh sách Kafka Brokers (Bắt buộc KAFKA_BROKERS)
	KafkaRawTopic     string   // Topic Kafka phát dữ liệu chuẩn (Bắt buộc KAFKA_RAW_TOPIC)
	KafkaInvalidTopic string   // Topic Kafka phát dữ liệu lỗi (Bắt buộc KAFKA_INVALID_TOPIC)
}

// LoadFromEnv nạp cấu hình từ Environment Variables (Tuyệt đối KHÔNG có fallback ngầm, Fail-Close hoàn toàn)
func LoadFromEnv() (*Config, error) {
	// Parse danh sách Kafka brokers từ KAFKA_BROKERS (phân tách bởi dấu phẩy)
	brokersStr := strings.TrimSpace(os.Getenv("KAFKA_BROKERS"))
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
		KaggleUsername:    strings.TrimSpace(os.Getenv("KAGGLE_USERNAME")),
		KaggleKey:         strings.TrimSpace(os.Getenv("KAGGLE_KEY")),
		DatasetSlug:       strings.TrimSpace(os.Getenv("KAGGLE_DATASET_SLUG")),
		SelectedFile:      strings.TrimSpace(os.Getenv("KAGGLE_SELECTED_FILE")),
		MinIOEndpoint:     strings.TrimSpace(os.Getenv("MINIO_ENDPOINT")),
		MinIOAccessKey:    strings.TrimSpace(os.Getenv("MINIO_ACCESS_KEY")),
		MinIOSecretKey:    strings.TrimSpace(os.Getenv("MINIO_SECRET_KEY")),
		MinIOBucket:       strings.TrimSpace(os.Getenv("MINIO_BUCKET")),
		MinIOUseSSL:       strings.TrimSpace(os.Getenv("MINIO_USE_SSL")) == "true",
		ForceDownload:     strings.TrimSpace(os.Getenv("FORCE_DOWNLOAD")) == "true",
		KafkaBrokers:      brokers,
		KafkaRawTopic:     strings.TrimSpace(os.Getenv("KAFKA_RAW_TOPIC")),
		KafkaInvalidTopic: strings.TrimSpace(os.Getenv("KAFKA_INVALID_TOPIC")),
	}

	// Thực hiện Validate nghiêm ngặt: Nếu thiếu bất kỳ biến môi trường nào -> Fail-Close lập tức!
	if err := cfg.Validate(); err != nil {
		return nil, fmt.Errorf("cấu hình thất bại (Fail-Close Active): %w", err)
	}

	return cfg, nil
}

// Validate kiểm tra tính bắt buộc của 100% các biến môi trường (Fail-Close 100%, ZERO Fallback)
func (c *Config) Validate() error {
	var missingVars []string

	if c.DatasetSlug == "" {
		missingVars = append(missingVars, "KAGGLE_DATASET_SLUG")
	}
	if c.SelectedFile == "" {
		missingVars = append(missingVars, "KAGGLE_SELECTED_FILE")
	}
	if c.MinIOEndpoint == "" {
		missingVars = append(missingVars, "MINIO_ENDPOINT")
	}
	if c.MinIOAccessKey == "" {
		missingVars = append(missingVars, "MINIO_ACCESS_KEY")
	}
	if c.MinIOSecretKey == "" {
		missingVars = append(missingVars, "MINIO_SECRET_KEY")
	}
	if c.MinIOBucket == "" {
		missingVars = append(missingVars, "MINIO_BUCKET")
	}
	if len(c.KafkaBrokers) == 0 {
		missingVars = append(missingVars, "KAFKA_BROKERS")
	}
	if c.KafkaRawTopic == "" {
		missingVars = append(missingVars, "KAFKA_RAW_TOPIC")
	}
	if c.KafkaInvalidTopic == "" {
		missingVars = append(missingVars, "KAFKA_INVALID_TOPIC")
	}

	// Nếu thiếu bất kỳ biến nào -> Fail-Close ngắt chương trình ngay lập tức
	if len(missingVars) > 0 {
		return fmt.Errorf("phát hiện %d biến môi trường chưa khai báo: [%s] (Fail-Close Rule Violated)", len(missingVars), strings.Join(missingVars, ", "))
	}

	return nil
}
