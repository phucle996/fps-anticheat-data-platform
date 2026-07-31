package router

import (
	"github.com/gin-gonic/gin"
	"go-api/internal/config"
	"go-api/internal/handler"
	"go-api/internal/service"
)

// SetupRouter khởi tạo Gin Engine, gắn Middlewares và định tuyến API v1 Routes
func SetupRouter(cfg *config.Config, services *service.ServiceContainer) *gin.Engine {
	gin.SetMode(gin.ReleaseMode)

	r := gin.New()
	r.Use(gin.Logger())
	r.Use(gin.Recovery())

	h := handler.NewHandler(cfg, services)

	v1 := r.Group("/api/v1")
	{
		v1.GET("/health", h.Health)
		v1.POST("/predict", h.Predict)
		v1.GET("/dataset/summary", h.DatasetSummary)
	}

	return r
}
