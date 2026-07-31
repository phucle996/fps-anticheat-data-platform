package service

import (
	"go-api/internal/client"
	"go-api/internal/domain"
)

// PredictService quản lý nghiệp vụ dự báo nguy cơ gian lận real-time
type PredictService struct {
	ipcClient *client.IPCClient
}

// NewPredictService khởi tạo PredictService
func NewPredictService(ipcClient *client.IPCClient) *PredictService {
	return &PredictService{
		ipcClient: ipcClient,
	}
}

// ExecutePredict chuyển tiếp request dự báo sang Dedicated Rust Inference Engine qua IPC
func (s *PredictService) ExecutePredict(req *domain.PredictRequest) (*domain.PredictResponse, error) {
	if req.Op == "" {
		req.Op = "predict"
	}
	return s.ipcClient.Predict(req)
}
