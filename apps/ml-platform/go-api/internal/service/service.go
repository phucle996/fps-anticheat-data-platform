package service

import (
	"github.com/phucle996/fps-anticheat/apps/ml-platform/go-api/internal/client"
	"github.com/phucle996/fps-anticheat/apps/ml-platform/go-api/internal/config"
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
