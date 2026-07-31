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

	"github.com/phucle996/fps-anticheat/apps/ml-platform/go-api/internal/config"
	"github.com/phucle996/fps-anticheat/apps/ml-platform/go-api/internal/handler"
	"github.com/phucle996/fps-anticheat/apps/ml-platform/go-api/internal/ipc"
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
	ipcClient := ipc.NewClient(cfg.IPCSocketPath)
	server := handler.NewServer(cfg, ipcClient)

	mux := http.NewServeMux()
	server.RegisterRoutes(mux)

	req := httptest.NewRequest(http.MethodGet, "/api/v1/health", nil)
	rec := httptest.NewRecorder()

	mux.ServeHTTP(rec, req)

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

		var req ipc.PredictRequest
		_ = json.NewDecoder(conn).Decode(&req)

		resp := ipc.PredictResponse{
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
	ipcClient := ipc.NewClient(socketPath)
	server := handler.NewServer(cfg, ipcClient)

	mux := http.NewServeMux()
	server.RegisterRoutes(mux)

	bodyReq := ipc.PredictRequest{
		Op:       "predict",
		MatchID:  "match_200",
		PlayerID: "player_B",
		Features: []float32{1.5, 140.0, 0.95, 120.0, 250.0, 800.0},
	}
	jsonBytes, _ := json.Marshal(bodyReq)

	req := httptest.NewRequest(http.MethodPost, "/api/v1/predict", bytes.NewReader(jsonBytes))
	rec := httptest.NewRecorder()

	mux.ServeHTTP(rec, req)

	if rec.Code != http.StatusOK {
		t.Fatalf("Kỳ vọng Status Code 200, nhưng nhận được %d: %s", rec.Code, rec.Body.String())
	}

	var resp ipc.PredictResponse
	if err := json.Unmarshal(rec.Body.Bytes(), &resp); err != nil {
		t.Fatalf("Lỗi giải mã JSON response: %v", err)
	}

	if resp.Status != "ok" || resp.RiskLevel != "CRITICAL" {
		t.Fatalf("Kết quả IPC Response không khớp kỳ vọng: %+v", resp)
	}
}
