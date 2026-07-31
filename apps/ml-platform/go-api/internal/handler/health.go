package handler

import (
	"net/http"
	"os"

	"github.com/gin-gonic/gin"
)

// Health kiểm tra sức khỏe Go API Gateway và kết nối file socket Unix Domain Socket IPC với Rust Engine
func (h *Handler) Health(c *gin.Context) {
	// Kiểm tra tính sẵn sàng của đường dẫn Unix Domain Socket IPC
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
