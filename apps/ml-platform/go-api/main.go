package main

import (
	"log"
	"net/http"

	"github.com/phucle996/fps-anticheat/apps/ml-platform/go-api/internal/config"
	"github.com/phucle996/fps-anticheat/apps/ml-platform/go-api/internal/handler"
	"github.com/phucle996/fps-anticheat/apps/ml-platform/go-api/internal/ipc"
)

func main() {
	log.Println("Bắt đầu khởi chạy Dịch vụ Go API Gateway...")

	// 1. Nạp cấu hình từ biến môi trường với cơ chế Fail-Close 100%
	cfg, err := config.FromEnv()
	if err != nil {
		log.Fatalf("[FAIL-CLOSE] Không thể khởi chạy Go API Gateway: %v", err)
	}

	// 2. Khởi tạo IPC Client kết nối Unix Domain Socket
	ipcClient := ipc.NewClient(cfg.IPCSocketPath)

	// 3. Khởi tạo REST Server Router
	server := handler.NewServer(cfg, ipcClient)
	mux := http.NewServeMux()
	server.RegisterRoutes(mux)

	addr := ":" + cfg.HTTPPort
	log.Printf("Go API Gateway đã khởi động thành công và sẵn sàng lắng nghe tại %s (Socket UDS IPC: %s)", addr, cfg.IPCSocketPath)

	if err := http.ListenAndServe(addr, mux); err != nil {
		log.Fatalf("Lỗi lắng nghe HTTP Server: %v", err)
	}
}
