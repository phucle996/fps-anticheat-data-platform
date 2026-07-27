package kaggle

import (
	"context"
	"fmt"
	"io"
	"net/http"
	"time"
)

// Client chịu trách nhiệm tải dataset archive từ Kaggle qua HTTP API
type Client struct {
	username   string       // Username xác thực
	key        string       // Key xác thực
	httpClient *http.Client // HTTP Client với timeout
}

// NewClient khởi tạo Kaggle Client
func NewClient(username, key string) *Client {
	return &Client{
		username: username,
		key:      key,
		httpClient: &http.Client{
			Timeout: 30 * time.Minute, // Timeout 30 phút cho việc tải dataset dung lượng lớn
		},
	}
}

// DownloadDatasetStream thực hiện yêu cầu HTTP GET để stream dữ liệu dataset zip từ Kaggle
func (c *Client) DownloadDatasetStream(ctx context.Context, datasetSlug string) (io.ReadCloser, error) {
	// URL API chính thức của Kaggle để tải xuống Zip Archive
	url := fmt.Sprintf("https://www.kaggle.com/api/v1/datasets/download/%s", datasetSlug)

	// Tạo HTTP Request với Context
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, url, nil)
	if err != nil {
		return nil, fmt.Errorf("không thể tạo HTTP request: %w", err)
	}

	// Đính kèm xác thực Basic Auth nếu user/key được cung cấp
	if c.username != "" && c.key != "" {
		req.SetBasicAuth(c.username, c.key)
	}

	// Thực thi HTTP Request
	resp, err := c.httpClient.Do(req)
	if err != nil {
		return nil, fmt.Errorf("lỗi kết nối khi tải dataset Kaggle: %w", err)
	}

	// Kiểm tra Status Code thành công (HTTP 200 OK)
	if resp.StatusCode != http.StatusOK {
		resp.Body.Close()
		return nil, fmt.Errorf("Kaggle API trả về lỗi HTTP status: %d (%s)", resp.StatusCode, resp.Status)
	}

	// Trả về ReadCloser stream dữ liệu cho bên gọi xử lý
	return resp.Body, nil
}
