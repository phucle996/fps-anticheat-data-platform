package domain

import "encoding/xml"

// ListBucketResult định nghĩa cấu trúc XML response từ MinIO S3 API (Hỗ trợ XML Namespace)
type ListBucketResult struct {
	XMLName  xml.Name `xml:"ListBucketResult"`
	Contents []struct {
		Key  string `xml:"Key"`
		Size int64  `xml:"Size"`
	} `xml:"Contents"`
}

// SummaryResponse định nghĩa chỉ số KPI trả về từ API /api/v1/dataset/summary
type SummaryResponse struct {
	Status             string `json:"status"`
	TotalRawRecords    int    `json:"total_raw_records"`
	TotalMatches       int    `json:"total_matches"`
	TotalPlayers       int    `json:"total_players"`
	TotalBatches       int    `json:"total_batches"`
	CleanSilverRecords int    `json:"clean_silver_records"`
	InvalidRecords     int    `json:"invalid_records"`
	PredictionCount    int    `json:"prediction_count"`
	HighRiskCount      int    `json:"high_risk_count"`
	ModelVersion       string `json:"model_version"`
	FeatureVersion     string `json:"feature_version"`
}
