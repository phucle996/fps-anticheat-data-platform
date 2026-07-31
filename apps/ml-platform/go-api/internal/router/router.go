package router

import (
	"github.com/gin-gonic/gin"
	"github.com/phucle996/fps-anticheat/apps/ml-platform/go-api/internal/config"
	"github.com/phucle996/fps-anticheat/apps/ml-platform/go-api/internal/handler"
	"github.com/phucle996/fps-anticheat/apps/ml-platform/go-api/internal/ipc"
)

// SetupRouter khởi tạo Gin Web Framework Engine, gắn Middlewares và đăng ký các REST API Endpoints
func SetupRouter(cfg *config.Config, ipcClient *ipc.Client) *gin.Engine {
	// Thiết lập Gin chạy ở chế độ Release Mode chuẩn Cloud-Native High-Performance
	gin.SetMode(gin.ReleaseMode)

	r := gin.New()

	// Gắn các Cloud-Native Standard Middlewares (Structured Logging & Panic Recovery)
	r.Use(gin.Logger())
	r.Use(gin.Recovery())

	// Khởi tạo Base Handler chứa các dependencies
	h := handler.NewHandler(cfg, ipcClient)

	// Định tuyến nhóm đường dẫn API v1 (/api/v1)
	v1 := r.Group("/api/v1")
	{
		v1.GET("/health", h.Health)
		v1.POST("/predict", h.Predict)
		v1.GET("/dataset/summary", h.DatasetSummary)
	}

	return r
}
