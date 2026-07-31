package service

import (
	"go-api/internal/client"
	"go-api/internal/config"
)

// ServiceContainer chứa tập hợp tất cả các dịch vụ Business Logic của Go API Gateway
type ServiceContainer struct {
	PredictPredictService *PredictService
	SummaryService        *SummaryService
}

// NewServiceContainer khởi tạo container chứa tất cả các Services
func NewServiceContainer(cfg *config.Config, ipcClient *client.IPCClient) *ServiceContainer {
	return &ServiceContainer{
		PredictPredictService: NewPredictService(ipcClient),
		SummaryService:        NewSummaryService(cfg, ipcClient),
	}
}
