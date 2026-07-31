package client

import (
	"encoding/json"
	"fmt"
	"net"
	"time"

	"go-api/internal/domain"
)

// IPCClient quản lý kết nối IPC Client Unix Domain Socket tới Rust Inference Engine
type IPCClient struct {
	socketPath string // Đường dẫn file socket (vd: "/tmp/rust_inference.sock")
}

// NewIPCClient khởi tạo IPC Client
func NewIPCClient(socketPath string) *IPCClient {
	return &IPCClient{
		socketPath: socketPath,
	}
}

// Predict thực thi gửi request JSON qua Unix Domain Socket IPC và nhận kết quả siêu tốc
func (c *IPCClient) Predict(req *domain.PredictRequest) (*domain.PredictResponse, error) {
	conn, err := net.DialTimeout("unix", c.socketPath, 2*time.Second)
	if err != nil {
		return nil, fmt.Errorf("không thể kết nối Unix Domain Socket tại '%s': %w", c.socketPath, err)
	}
	defer conn.Close()

	encoder := json.NewEncoder(conn)
	if err := encoder.Encode(req); err != nil {
		return nil, fmt.Errorf("lỗi mã hóa JSON IPC request: %w", err)
	}

	var resp domain.PredictResponse
	decoder := json.NewDecoder(conn)
	if err := decoder.Decode(&resp); err != nil {
		return nil, fmt.Errorf("lỗi giải mã JSON IPC response từ Rust Engine: %w", err)
	}

	return &resp, nil
}

// GetActiveModelVersion thực hiện truy vấn phiên bản mô hình ML đang kích hoạt từ Rust Engine qua IPC
func (c *IPCClient) GetActiveModelVersion() string {
	resp, err := c.Predict(&domain.PredictRequest{
		Op:       "predict",
		MatchID:  "health_check",
		PlayerID: "health_check",
		Features: []float32{0, 0, 0, 0, 0},
	})
	if err == nil && resp != nil && resp.ModelVersion != "" && resp.ModelVersion != "UNAVAILABLE" {
		return resp.ModelVersion
	}
	return "UNAVAILABLE"
}
