package ipc

import (
	"encoding/json"
	"fmt"
	"net"
	"time"
)

// PredictRequest định nghĩa cấu trúc Yêu cầu dự báo truyền qua Unix Domain Socket IPC
type PredictRequest struct {
	Op       string     `json:"op"`        // Operation name (vd: "predict")
	MatchID  string     `json:"match_id"`  // Mã trận đấu
	PlayerID string     `json:"player_id"` // Mã người chơi
	Features [6]float32 `json:"features"`  // 6 đặc trưng Gold Feature Contract
}

// PredictResponse định nghĩa cấu trúc Phản hồi kết quả dự báo từ Rust Inference Engine
type PredictResponse struct {
	Status       string  `json:"status"`        // Trạng thái ("ok" hoặc "error")
	MatchID      string  `json:"match_id"`      // Mã trận đấu
	PlayerID     string  `json:"player_id"`     // Mã người chơi
	RiskScore    float32 `json:"risk_score"`    // Anomaly Risk Score (0.0 - 1.0)
	RiskLevel    string  `json:"risk_level"`    // Nhãn Risk Level ("LOW", "MEDIUM", "HIGH", "CRITICAL")
	ModelVersion string  `json:"model_version"` // Phiên bản ONNX Model ("v1")
}

// Client quản lý kết nối IPC Client Unix Domain Socket tới Rust Inference Engine
type Client struct {
	socketPath string // Đường dẫn file socket (vd: "/tmp/rust_inference.sock")
}

// NewClient khởi tạo IPC Client
func NewClient(socketPath string) *Client {
	return &Client{
		socketPath: socketPath,
	}
}

// Predict thực thi gửi request JSON qua Unix Domain Socket IPC và nhận kết quả siêu tốc
func (c *Client) Predict(req *PredictRequest) (*PredictResponse, error) {
	conn, err := net.DialTimeout("unix", c.socketPath, 2*time.Second)
	if err != nil {
		return nil, fmt.Errorf("không thể kết nối Unix Domain Socket tại '%s': %w", c.socketPath, err)
	}
	defer conn.Close()

	// 1. Ghi JSON payload request
	encoder := json.NewEncoder(conn)
	if err := encoder.Encode(req); err != nil {
		return nil, fmt.Errorf("lỗi mã hóa JSON IPC request: %w", err)
	}

	// 2. Đọc JSON payload response
	var resp PredictResponse
	decoder := json.NewDecoder(conn)
	if err := decoder.Decode(&resp); err != nil {
		return nil, fmt.Errorf("lỗi giải mã JSON IPC response từ Rust Engine: %w", err)
	}

	return &resp, nil
}
