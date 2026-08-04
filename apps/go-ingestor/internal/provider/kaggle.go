package provider

import (
	"context"
	"fmt"
	"io"
	"net/http"
	"time"
)

// KaggleClient tương tác với Kaggle REST API v1 để lấy thông tin và tải dataset telemetry
type KaggleClient struct {
	username   string       // Tên người dùng Kaggle (KAGGLE_USERNAME)
	apiKey     string       // Kaggle API Key (KAGGLE_KEY)
	httpClient *http.Client // HTTP Client với Timeout an toàn cho tải file lớn
}

// NewKaggleClient khởi tạo KaggleClient
func NewKaggleClient(username, apiKey string) *KaggleClient {
	return &KaggleClient{
		username: username,
		apiKey:   apiKey,
		httpClient: &http.Client{
			Timeout: 30 * time.Minute, // Timeout 30 phút phục vụ tải dataset lớn hàng GB
		},
	}
}

// DownloadDatasetStream mở luồng HTTP Stream tải file zip của Kaggle Dataset và trả về ContentLength
func (k *KaggleClient) DownloadDatasetStream(ctx context.Context, datasetSlug string) (io.ReadCloser, int64, error) {
	url := fmt.Sprintf("https://www.kaggle.com/api/v1/datasets/download/%s", datasetSlug)
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, url, nil)
	if err != nil {
		return nil, 0, fmt.Errorf("tạo HTTP request thất bại: %w", err)
	}

	// Xác thực Basic Auth với Kaggle Username & API Key
	req.SetBasicAuth(k.username, k.apiKey)

	resp, err := k.httpClient.Do(req)
	if err != nil {
		return nil, 0, fmt.Errorf("gửi request tới Kaggle API thất bại: %w", err)
	}

	if resp.StatusCode != http.StatusOK {
		resp.Body.Close()
		return nil, 0, fmt.Errorf("kaggle API trả về mã lỗi HTTP: %d %s", resp.StatusCode, resp.Status)
	}

	return resp.Body, resp.ContentLength, nil
}
