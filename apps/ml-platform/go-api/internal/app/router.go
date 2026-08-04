package app

import (
	"github.com/gin-gonic/gin"
)

// SetupRouter khởi tạo Gin Engine, gắn Middlewares và định tuyến API v1 Routes thông qua Module container
func SetupRouter(mod *Module) *gin.Engine {
	// Đặt chế độ Release Mode cho Gin Framework để tối ưu hiệu năng
	gin.SetMode(gin.ReleaseMode)

	r := gin.New()
	r.Use(gin.Logger())   // Logging Middleware chuẩn hóa log request
	r.Use(gin.Recovery()) // Recovery Middleware tự động bắt panic chống crash server

	// Định tuyến nhóm API v1 Endpoints trực tiếp từ từng Handler trong Module
	v1 := r.Group("/api/v1")
	{
		v1.GET("/health", mod.HealthHandler.Health)
		v1.POST("/predict", mod.PredictHandler.Predict)
		v1.GET("/dataset/summary", mod.SummaryHandler.DatasetSummary)
	}

	return r
}
