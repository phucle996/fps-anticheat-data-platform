package service

import (
	"go-api/internal/infra"
	"go-api/internal/model"
)

// PredictService quản lý nghiệp vụ dự báo nguy cơ gian lận real-time
type PredictService struct {
	ipcClient *infra.IPCClient
}

// NewPredictService khởi tạo PredictService
func NewPredictService(ipcClient *infra.IPCClient) *PredictService {
	return &PredictService{
		ipcClient: ipcClient,
	}
}

// ExecutePredict chuyển tiếp request dự báo sang Dedicated Rust Inference Engine qua IPC
func (s *PredictService) ExecutePredict(req *model.PredictRequest) (*model.PredictResponse, error) {
	if req.Op == "" {
		req.Op = "predict"
	}
	return s.ipcClient.Predict(req)
}
