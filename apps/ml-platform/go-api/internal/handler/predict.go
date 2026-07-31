package handler

import (
	"net/http"

	"github.com/gin-gonic/gin"
	"github.com/phucle996/fps-anticheat/apps/ml-platform/go-api/internal/ipc"
)

// Predict tiếp nhận REST request dự báo real-time và chuyển tiếp sang Dedicated Rust Inference Engine qua UDS IPC
func (h *Handler) Predict(c *gin.Context) {
	var req ipc.PredictRequest
	// Binding JSON request đầu vào từ client với cơ chế tự động validate của Gin
	if err := c.ShouldBindJSON(&req); err != nil {
		c.JSON(http.StatusBadRequest, gin.H{
			"status": "error",
			"error":  "Định dạng JSON request không hợp lệ: " + err.Error(),
		})
		return
	}

	if req.Op == "" {
		req.Op = "predict"
	}

	// Gọi truyền dữ liệu qua đường ống High-Performance Unix Domain Socket IPC với Rust Engine
	resp, err := h.ipcClient.Predict(&req)
	if err != nil {
		c.JSON(http.StatusInternalServerError, gin.H{
			"status": "error",
			"error":  "Lỗi giao tiếp Unix Domain Socket IPC với Rust Engine: " + err.Error(),
		})
		return
	}

	// Trả về HTTP 503 Service Unavailable nếu mô hình ONNX ở trạng thái UNAVAILABLE
	if resp.Status == "UNAVAILABLE" || resp.RiskLevel == "UNAVAILABLE" {
		c.JSON(http.StatusServiceUnavailable, resp)
		return
	}

	c.JSON(http.StatusOK, resp)
}
