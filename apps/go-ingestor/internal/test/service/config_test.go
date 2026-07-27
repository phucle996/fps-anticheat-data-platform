package service_test

import (
	"os"
	"testing"

	"pubg-anti-cheat/go-ingestor/internal/config"
)

// TestConfig_FailCloseWhenMissingEnv kiểm tra quy tắc Fail-Close ngắt chương trình khi thiếu biến môi trường
func TestConfig_FailCloseWhenMissingEnv(t *testing.T) {
	// Xóa sạch các biến môi trường
	os.Unsetenv("KAGGLE_DATASET_SLUG")
	os.Unsetenv("MINIO_ENDPOINT")
	os.Unsetenv("KAFKA_BROKERS")

	_, err := config.LoadFromEnv()
	if err == nil {
		t.Fatalf("Kỳ vọng LoadFromEnv trả về lỗi Fail-Close khi thiếu biến môi trường nhưng nhận được nil")
	}
}

// TestConfig_SuccessWhenAllEnvSet kiểm tra nạp thành công khi cung cấp đầy đủ 100% các biến môi trường
func TestConfig_SuccessWhenAllEnvSet(t *testing.T) {
	// Thiết lập đầy đủ biến môi trường
	os.Setenv("KAGGLE_USERNAME", "testuser")
	os.Setenv("KAGGLE_KEY", "testkey")
	os.Setenv("KAGGLE_DATASET_SLUG", "test/slug")
	os.Setenv("KAGGLE_SELECTED_FILE", "train.csv")
	os.Setenv("MINIO_ENDPOINT", "localhost:9000")
	os.Setenv("MINIO_ACCESS_KEY", "minioadmin")
	os.Setenv("MINIO_SECRET_KEY", "minioadmin")
	os.Setenv("MINIO_BUCKET", "pubg-data")
	os.Setenv("KAFKA_BROKERS", "localhost:9092")
	os.Setenv("KAFKA_RAW_TOPIC", "pubg.v1.player-stat.raw")
	os.Setenv("KAFKA_INVALID_TOPIC", "pubg.v1.invalid")

	defer func() {
		os.Unsetenv("KAGGLE_USERNAME")
		os.Unsetenv("KAGGLE_KEY")
		os.Unsetenv("KAGGLE_DATASET_SLUG")
		os.Unsetenv("KAGGLE_SELECTED_FILE")
		os.Unsetenv("MINIO_ENDPOINT")
		os.Unsetenv("MINIO_ACCESS_KEY")
		os.Unsetenv("MINIO_SECRET_KEY")
		os.Unsetenv("MINIO_BUCKET")
		os.Unsetenv("KAFKA_BROKERS")
		os.Unsetenv("KAFKA_RAW_TOPIC")
		os.Unsetenv("KAFKA_INVALID_TOPIC")
	}()

	cfg, err := config.LoadFromEnv()
	if err != nil {
		t.Fatalf("LoadFromEnv trả về lỗi không mong muốn: %v", err)
	}

	if cfg.DatasetSlug != "test/slug" || cfg.KafkaRawTopic != "pubg.v1.player-stat.raw" {
		t.Errorf("Giá trị nạp từ env không chính xác: %+v", cfg)
	}
}
