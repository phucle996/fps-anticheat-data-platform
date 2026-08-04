package handler

import (
	"net/http"

	"github.com/gin-gonic/gin"
	"go-api/internal/service"
)

// SummaryHandler quản lý HTTP Handler tổng hợp KPI dữ liệu Data Lake
type SummaryHandler struct {
	summaryService *service.SummaryService
}

// NewSummaryHandler khởi tạo SummaryHandler với SummaryService dependency
func NewSummaryHandler(summaryService *service.SummaryService) *SummaryHandler {
	return &SummaryHandler{
		summaryService: summaryService,
	}
}

// DatasetSummary xử lý HTTP REST request và ủy quyền tổng hợp KPI cho SummaryService
func (h *SummaryHandler) DatasetSummary(c *gin.Context) {
	summary := h.summaryService.GetDatasetSummary(c.Request.Context())
	c.JSON(http.StatusOK, summary)
}
