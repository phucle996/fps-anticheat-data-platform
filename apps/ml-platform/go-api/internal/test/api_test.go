package test

import (
	"bytes"
	"encoding/json"
	"net"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"testing"
	"time"

	"go-api/internal/app"
	"go-api/internal/config"
	"go-api/internal/handler"
	"go-api/internal/infra"
	"go-api/internal/model"
	"go-api/internal/service"
)

// TestConfigFailClose kiểm tra cơ chế Fail-Close 100% khi thiếu biến môi trường
func TestConfigFailClose(t *testing.T) {
	os.Unsetenv("HTTP_PORT")
	_, err := config.FromEnv()
	if err == nil {
		t.Fatalf("Bắt buộc phải báo lỗi Fail-Close khi thiếu HTTP_PORT")
	}
}

// TestHealthEndpoint kiểm tra REST Endpoint /api/v1/health
func TestHealthEndpoint(t *testing.T) {
	cfg := &config.Config{
		HTTPPort:      "8081",
		IPCSocketPath: "/tmp/non_existent.sock",
	}
	minioClient := infra.NewMinIOClient(cfg)
	ipcClient := infra.NewIPCClient(cfg)
	predictService := service.NewPredictService(ipcClient)
	summaryService := service.NewSummaryService(cfg, minioClient, ipcClient)

	mod := &app.Module{
		Cfg:            cfg,
		MinIOClient:    minioClient,
		IPCClient:      ipcClient,
		PredictService: predictService,
		SummaryService: summaryService,
		HealthHandler:  handler.NewHealthHandler(cfg),
		PredictHandler: handler.NewPredictHandler(predictService),
		SummaryHandler: handler.NewSummaryHandler(summaryService),
	}

	r := app.SetupRouter(mod)

	req := httptest.NewRequest(http.MethodGet, "/api/v1/health", nil)
	rec := httptest.NewRecorder()

	r.ServeHTTP(rec, req)

	if rec.Code != http.StatusOK {
		t.Fatalf("Kỳ vọng Status Code 200, nhưng nhận được %d", rec.Code)
	}

	var resp map[string]interface{}
	if err := json.Unmarshal(rec.Body.Bytes(), &resp); err != nil {
		t.Fatalf("Lỗi giải mã JSON response: %v", err)
	}

	if resp["status"] != "UP" {
		t.Fatalf("Kỳ vọng status UP, nhận được %v", resp["status"])
	}
}

// TestPredictIPCFlow kiểm tra toàn bộ luồng truyền nhận giữa Go API và Mock UDS IPC Server
func TestPredictIPCFlow(t *testing.T) {
	socketPath := filepath.Join(os.TempDir(), "test_go_api.sock")
	_ = os.Remove(socketPath)

	// Khởi tạo mock Unix Domain Socket Server
	listener, err := net.Listen("unix", socketPath)
	if err != nil {
		t.Fatalf("Không thể khởi tạo mock UDS Listener: %v", err)
	}
	defer listener.Close()
	defer os.Remove(socketPath)

	go func() {
		conn, err := listener.Accept()
		if err != nil {
			return
		}
		defer conn.Close()

		var req model.PredictRequest
		_ = json.NewDecoder(conn).Decode(&req)

		resp := model.PredictResponse{
			Status:       "ok",
			MatchID:      req.MatchID,
			PlayerID:     req.PlayerID,
			RiskScore:    0.92,
			RiskLevel:    "CRITICAL",
			ModelVersion: "v1",
		}
		_ = json.NewEncoder(conn).Encode(resp)
	}()

	time.Sleep(50 * time.Millisecond)

	cfg := &config.Config{
		HTTPPort:      "8081",
		IPCSocketPath: socketPath,
	}
	minioClient := infra.NewMinIOClient(cfg)
	ipcClient := infra.NewIPCClient(cfg)
	predictService := service.NewPredictService(ipcClient)
	summaryService := service.NewSummaryService(cfg, minioClient, ipcClient)

	mod := &app.Module{
		Cfg:            cfg,
		MinIOClient:    minioClient,
		IPCClient:      ipcClient,
		PredictService: predictService,
		SummaryService: summaryService,
		HealthHandler:  handler.NewHealthHandler(cfg),
		PredictHandler: handler.NewPredictHandler(predictService),
		SummaryHandler: handler.NewSummaryHandler(summaryService),
	}

	r := app.SetupRouter(mod)

	bodyReq := model.PredictRequest{
		Op:       "predict",
		MatchID:  "match_200",
		PlayerID: "player_B",
		Features: []float32{1.5, 140.0, 0.95, 120.0, 250.0, 800.0},
	}
	jsonBytes, _ := json.Marshal(bodyReq)

	req := httptest.NewRequest(http.MethodPost, "/api/v1/predict", bytes.NewReader(jsonBytes))
	rec := httptest.NewRecorder()

	r.ServeHTTP(rec, req)

	if rec.Code != http.StatusOK {
		t.Fatalf("Kỳ vọng Status Code 200, nhưng nhận được %d: %s", rec.Code, rec.Body.String())
	}

	var resp model.PredictResponse
	if err := json.Unmarshal(rec.Body.Bytes(), &resp); err != nil {
		t.Fatalf("Lỗi giải mã JSON response: %v", err)
	}

	if resp.Status != "ok" || resp.RiskLevel != "CRITICAL" {
		t.Fatalf("Kết quả IPC Response không khớp kỳ vọng: %+v", resp)
	}
}
