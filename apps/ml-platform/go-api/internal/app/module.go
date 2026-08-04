package app

import (
	"fmt"

	"go-api/internal/config"
	"go-api/internal/handler"
	"go-api/internal/infra"
	"go-api/internal/service"
)

// Module tập trung khởi tạo và lưu trữ tất cả các Constructors (Config, Infra Clients, Services, Handlers)
// Đóng vai trò là IoC Container duy nhất Wire các thành phần ứng dụng lại với nhau
type Module struct {
	Cfg            *config.Config
	MinIOClient    *infra.MinIOClient
	IPCClient      *infra.IPCClient
	PredictService *service.PredictService
	SummaryService *service.SummaryService
	HealthHandler  *handler.HealthHandler
	PredictHandler *handler.PredictHandler
	SummaryHandler *handler.SummaryHandler
}

// NewModule khởi tạo tập trung toàn bộ các dependencies từ Config -> Infra -> Services -> Handlers
func NewModule() (*Module, error) {
	// 1. Khởi tạo Config từ biến môi trường (Fail-Close 100%)
	cfg, err := config.FromEnv()
	if err != nil {
		return nil, fmt.Errorf("[FAIL-CLOSE] Nạp biến môi trường thất bại: %w", err)
	}

	// 2. Khởi tạo các Infrastructure Clients từ infra package (MinIO S3 Client, IPC UDS Client)
	minioClient := infra.NewMinIOClient(cfg)
	ipcClient := infra.NewIPCClient(cfg)

	// 3. Khởi tạo các Services thuộc tầng Business Logic Layer
	predictService := service.NewPredictService(ipcClient)
	summaryService := service.NewSummaryService(cfg, minioClient, ipcClient)

	// 4. Khởi tạo từng Handler riêng biệt thông qua constructor của từng handler
	healthHandler := handler.NewHealthHandler(cfg)
	predictHandler := handler.NewPredictHandler(predictService)
	summaryHandler := handler.NewSummaryHandler(summaryService)

	return &Module{
		Cfg:            cfg,
		MinIOClient:    minioClient,
		IPCClient:      ipcClient,
		PredictService: predictService,
		SummaryService: summaryService,
		HealthHandler:  healthHandler,
		PredictHandler: predictHandler,
		SummaryHandler: summaryHandler,
	}, nil
}
