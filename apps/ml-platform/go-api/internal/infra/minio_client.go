package infra

import (
	"context"
	"encoding/json"
	"encoding/xml"
	"fmt"
	"io"
	"net/http"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"go-api/internal/config"
	"go-api/internal/model"
)

// ManifestContent đại diện cho cấu trúc JSON của BatchManifest lưu trên MinIO S3
type ManifestContent struct {
	TotalRecordsRead      int `json:"total_records_read"`
	ValidRecordsCount     int `json:"valid_records_count"`
	InvalidRecordsCount   int `json:"invalid_records_count"`
	DuplicateRecordsCount int `json:"duplicate_records_count"`
}

// SummaryDataAggregated đại diện cho dữ liệu tổng hợp KPI đọc từ MinIO S3
type SummaryDataAggregated struct {
	TotalRaw       int
	CleanSilver    int
	InvalidRecords int
	TotalBatches   int
}

// MinIOClient quản lý việc kết nối và thao tác với MinIO S3 REST API
// Tối ưu hóa với HTTP Connection Pool và Concurrent Worker Pool để fetch dữ liệu song song
type MinIOClient struct {
	cfg        *config.Config
	httpClient *http.Client
}

// NewMinIOClient khởi tạo MinIOClient từ Config với HTTP Transport tùy chỉnh (Timeouts & Connection Pool)
func NewMinIOClient(cfg *config.Config) *MinIOClient {
	transport := &http.Transport{
		MaxIdleConns:        100,
		MaxIdleConnsPerHost: 100,
		IdleConnTimeout:     90 * time.Second,
	}

	client := &http.Client{
		Transport: transport,
		Timeout:   5 * time.Second, // Timeout nghiêm ngặt 5s bảo vệ Goroutines khỏi bị leak
	}

	return &MinIOClient{
		cfg:        cfg,
		httpClient: client,
	}
}

// FetchSummaryData lấy danh sách manifests và fetch nội dung song song qua Worker Pool
func (m *MinIOClient) FetchSummaryData(ctx context.Context) (*SummaryDataAggregated, error) {
	// 1. Truy vấn danh sách Objects thuộc prefix manifests/ bằng MinIO S3 REST API (list-type=2)
	s3Endpoint := fmt.Sprintf("%s/%s?list-type=2&prefix=manifests/", m.cfg.MinIOEndpoint, m.cfg.MinIOBucketData)
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, s3Endpoint, nil)
	if err != nil {
		return nil, fmt.Errorf("tạo HTTP request tới S3 list-objects thất bại: %w", err)
	}

	if m.cfg.MinIOAccessKey != "" && m.cfg.MinIOSecretKey != "" {
		req.SetBasicAuth(m.cfg.MinIOAccessKey, m.cfg.MinIOSecretKey)
	}

	resp, err := m.httpClient.Do(req)
	if err != nil {
		return nil, fmt.Errorf("gửi request tới MinIO S3 thất bại: %w", err)
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		return nil, fmt.Errorf("minIO S3 trả về HTTP error status: %d", resp.StatusCode)
	}

	bodyBytes, err := io.ReadAll(resp.Body)
	if err != nil {
		return nil, fmt.Errorf("lỗi đọc body response S3: %w", err)
	}

	var s3Result model.ListBucketResult
	if err := xml.Unmarshal(bodyBytes, &s3Result); err != nil {
		return nil, fmt.Errorf("giải mã XML ListBucketResult thất bại: %w", err)
	}

	// 2. Lọc ra danh sách các object keys là file JSON manifest
	var jsonKeys []string
	for _, obj := range s3Result.Contents {
		if strings.HasSuffix(obj.Key, ".json") && !strings.Contains(obj.Key, "checkpoint") {
			jsonKeys = append(jsonKeys, obj.Key)
		}
	}

	if len(jsonKeys) == 0 {
		return &SummaryDataAggregated{}, nil
	}

	// 3. Thực thi Concurrent Worker Pool để fetch & decode nội dung từng Manifest JSON song song
	var totalRaw atomic.Int64
	var cleanSilver atomic.Int64
	var invalidRecords atomic.Int64

	workerCount := 10
	if len(jsonKeys) < workerCount {
		workerCount = len(jsonKeys)
	}

	keysCh := make(chan string, len(jsonKeys))
	for _, k := range jsonKeys {
		keysCh <- k
	}
	close(keysCh)

	var wg sync.WaitGroup
	for i := 0; i < workerCount; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for objectKey := range keysCh {
				content, err := m.downloadManifest(ctx, objectKey)
				if err == nil && content != nil {
					totalRaw.Add(int64(content.TotalRecordsRead))
					cleanSilver.Add(int64(content.ValidRecordsCount))
					invalidRecords.Add(int64(content.InvalidRecordsCount + content.DuplicateRecordsCount))
				}
			}
		}()
	}
	wg.Wait()

	return &SummaryDataAggregated{
		TotalRaw:       int(totalRaw.Load()),
		CleanSilver:    int(cleanSilver.Load()),
		InvalidRecords: int(invalidRecords.Load()),
		TotalBatches:   len(jsonKeys),
	}, nil
}

// downloadManifest đọc và decode JSON của một manifest object cụ thể
func (m *MinIOClient) downloadManifest(ctx context.Context, objectKey string) (*ManifestContent, error) {
	manifestURL := fmt.Sprintf("%s/%s/%s", m.cfg.MinIOEndpoint, m.cfg.MinIOBucketData, objectKey)
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, manifestURL, nil)
	if err != nil {
		return nil, err
	}

	if m.cfg.MinIOAccessKey != "" && m.cfg.MinIOSecretKey != "" {
		req.SetBasicAuth(m.cfg.MinIOAccessKey, m.cfg.MinIOSecretKey)
	}

	resp, err := m.httpClient.Do(req)
	if err != nil {
		return nil, err
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		return nil, fmt.Errorf("tải manifest %s lỗi: HTTP %d", objectKey, resp.StatusCode)
	}

	var content ManifestContent
	if err := json.NewDecoder(resp.Body).Decode(&content); err != nil {
		return nil, err
	}

	return &content, nil
}
