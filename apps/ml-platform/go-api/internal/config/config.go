package config

import (
	"fmt"
	"os"
)

// Config lưu trữ thông tin cấu hình cho Go API Gateway với nguyên tắc Fail-Close 100% (Zero Fallback)
type Config struct {
	HTTPPort         string // Cổng HTTP REST (vd: "8081")
	IPCSocketPath    string // Đường dẫn file Unix Domain Socket IPC (vd: "/tmp/rust_inference.sock")
	MinIOEndpoint    string // Endpoint S3 MinIO (vd: "http://localhost:9000")
	MinIOBucketData  string // Bucket Data Lake (vd: "fps-anticheat-datalake")
	MinIOAccessKey   string // Access Key MinIO S3
	MinIOSecretKey   string // Secret Key MinIO S3
}

// FromEnv nạp biến môi trường Fail-Close 100% (Ném ra error nếu thiếu biến)
func FromEnv() (*Config, error) {
	httpPort, err := getRequiredEnv("HTTP_PORT")
	if err != nil {
		return nil, err
	}
	ipcSocketPath, err := getRequiredEnv("IPC_SOCKET_PATH")
	if err != nil {
		return nil, err
	}
	minioEndpoint, err := getRequiredEnv("MINIO_ENDPOINT")
	if err != nil {
		return nil, err
	}
	minioBucketData, err := getRequiredEnv("MINIO_BUCKET_DATA")
	if err != nil {
		return nil, err
	}
	minioAccessKey, err := getRequiredEnv("MINIO_ACCESS_KEY")
	if err != nil {
		return nil, err
	}
	minioSecretKey, err := getRequiredEnv("MINIO_SECRET_KEY")
	if err != nil {
		return nil, err
	}

	return &Config{
		HTTPPort:        httpPort,
		IPCSocketPath:   ipcSocketPath,
		MinIOEndpoint:   minioEndpoint,
		MinIOBucketData: minioBucketData,
		MinIOAccessKey:  minioAccessKey,
		MinIOSecretKey:  minioSecretKey,
	}, nil
}

// getRequiredEnv bắt buộc biến môi trường không được rỗng
func getRequiredEnv(key string) (string, error) {
	val := os.Getenv(key)
	if val == "" {
		return "", fmt.Errorf("[FAIL-CLOSE TRIGGERED] Thiếu biến môi trường bắt buộc '%s'", key)
	}
	return val, nil
}
