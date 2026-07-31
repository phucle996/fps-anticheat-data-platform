package handler

import (
	"net/http"

	"github.com/gin-gonic/gin"
)

// DatasetSummary xử lý HTTP REST request và ủy quyền tổng hợp KPI cho SummaryService
func (h *Handler) DatasetSummary(c *gin.Context) {
	summary := h.services.SummaryService.GetDatasetSummary(c.Request.Context())
	c.JSON(http.StatusOK, summary)
}
