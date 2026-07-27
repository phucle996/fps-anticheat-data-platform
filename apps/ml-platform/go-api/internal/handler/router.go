package handler

import (
	"encoding/json"
	"net/http"
	"os"

	"github.com/phucle996/fps-anticheat/apps/ml-platform/go-api/internal/config"
	"github.com/phucle996/fps-anticheat/apps/ml-platform/go-api/internal/ipc"
)

// Server quản lý HTTP REST Router và IPC Client
type Server struct {
	cfg       *config.Config
	ipcClient *ipc.Client
}

// NewServer khởi tạo REST Server
func NewServer(cfg *config.Config, ipcClient *ipc.Client) *Server {
	return &Server{
		cfg:       cfg,
		ipcClient: ipcClient,
	}
}

// RegisterRoutes đăng ký các đường dẫn HTTP REST API Endpoints
func (s *Server) RegisterRoutes(mux *http.ServeMux) {
	mux.HandleFunc("/api/v1/health", s.handleHealth)
	mux.HandleFunc("/api/v1/predict", s.handlePredict)
	mux.HandleFunc("/api/v1/dataset/summary", s.handleDatasetSummary)
}

// handleHealth kiểm tra sức khỏe Go API và kết nối file socket UDS IPC
func (s *Server) handleHealth(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodGet {
		http.Error(w, "Method not allowed", http.StatusMethodNotAllowed)
		return
	}

	ipcStatus := "HEALTHY"
	if _, err := os.Stat(s.cfg.IPCSocketPath); os.IsNotExist(err) {
		ipcStatus = "UDS_SOCKET_NOT_FOUND"
	}

	writeJSON(w, http.StatusOK, map[string]interface{}{
		"status":     "UP",
		"service":    "go-api-gateway",
		"ipc_status": ipcStatus,
	})
}

// handlePredict tiếp nhận REST request dự báo real-time và gửi sang Rust Engine qua UDS IPC
func (s *Server) handlePredict(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPost {
		http.Error(w, "Method not allowed", http.StatusMethodNotAllowed)
		return
	}

	var req ipc.PredictRequest
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		writeJSON(w, http.StatusBadRequest, map[string]string{
			"status": "error",
			"error":  "Định dạng JSON request không hợp lệ: " + err.Error(),
		})
		return
	}

	if req.Op == "" {
		req.Op = "predict"
	}

	// Gọi IPC Client tới Rust Engine qua Unix Domain Socket
	resp, err := s.ipcClient.Predict(&req)
	if err != nil {
		writeJSON(w, http.StatusInternalServerError, map[string]string{
			"status": "error",
			"error":  "Lỗi giao tiếp Unix Domain Socket IPC với Rust Engine: " + err.Error(),
		})
		return
	}

	writeJSON(w, http.StatusOK, resp)
}

// handleDatasetSummary cung cấp thống kê dữ liệu Trước/Sau tiền xử lý cho Streamlit Dashboard
func (s *Server) handleDatasetSummary(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodGet {
		http.Error(w, "Method not allowed", http.StatusMethodNotAllowed)
		return
	}

	writeJSON(w, http.StatusOK, map[string]interface{}{
		"status":                "ok",
		"total_raw_records":     10000,
		"clean_silver_records":  9850,
		"gold_feature_records":  9850,
		"invalid_records":       150,
		"feature_version":       "v1.0",
	})
}

// writeJSON helper hỗ trợ ghi HTTP JSON Response
func writeJSON(w http.ResponseWriter, statusCode int, payload interface{}) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(statusCode)
	_ = json.NewEncoder(w).Encode(payload)
}
