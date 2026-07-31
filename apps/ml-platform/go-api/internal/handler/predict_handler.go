package handler

import (
	"net/http"

	"github.com/gin-gonic/gin"
	"go-api/internal/domain"
)

// Predict tiếp nhận HTTP REST request và ủy quyền xử lý cho PredictService
func (h *Handler) Predict(c *gin.Context) {
	var req domain.PredictRequest
	if err := c.ShouldBindJSON(&req); err != nil {
		c.JSON(http.StatusBadRequest, gin.H{
			"status": "error",
			"error":  "Định dạng JSON request không hợp lệ: " + err.Error(),
		})
		return
	}

	resp, err := h.services.PredictPredictService.ExecutePredict(&req)
	if err != nil {
		c.JSON(http.StatusInternalServerError, gin.H{
			"status": "error",
			"error":  "Lỗi giao tiếp Unix Domain Socket IPC với Rust Engine: " + err.Error(),
		})
		return
	}

	if resp.Status == "UNAVAILABLE" || resp.RiskLevel == "UNAVAILABLE" {
		c.JSON(http.StatusServiceUnavailable, resp)
		return
	}

	c.JSON(http.StatusOK, resp)
}
