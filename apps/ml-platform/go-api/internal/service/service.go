package service

import (
	"go-api/internal/config"
	"go-api/internal/infra"
)

// ServiceContainer chứa tập hợp tất cả các dịch vụ Business Logic của Go API Gateway
type ServiceContainer struct {
	PredictPredictService *PredictService
	SummaryService        *SummaryService
}

// NewServiceContainer khởi tạo container chứa tất cả các Services
func NewServiceContainer(cfg *config.Config, minioClient *infra.MinIOClient, ipcClient *infra.IPCClient) *ServiceContainer {
	return &ServiceContainer{
		PredictPredictService: NewPredictService(ipcClient),
		SummaryService:        NewSummaryService(cfg, minioClient, ipcClient),
	}
}
