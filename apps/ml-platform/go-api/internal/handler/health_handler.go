package handler

import (
	"net/http"
	"os"

	"github.com/gin-gonic/gin"
	"go-api/internal/config"
)

// HealthHandler quản lý HTTP Handler kiểm tra sức khỏe hệ thống
type HealthHandler struct {
	cfg *config.Config
}

// NewHealthHandler khởi tạo HealthHandler với Config dependency
func NewHealthHandler(cfg *config.Config) *HealthHandler {
	return &HealthHandler{
		cfg: cfg,
	}
}

// Health kiểm tra trạng thái hoạt động của Go API Gateway và kiểm tra sự tồn tại của UDS IPC Socket
func (h *HealthHandler) Health(c *gin.Context) {
	ipcStatus := "HEALTHY"
	if _, err := os.Stat(h.cfg.IPCSocketPath); os.IsNotExist(err) {
		ipcStatus = "UDS_SOCKET_NOT_FOUND"
	}

	c.JSON(http.StatusOK, gin.H{
		"status":     "UP",
		"service":    "go-api-gateway",
		"ipc_status": ipcStatus,
	})
}
