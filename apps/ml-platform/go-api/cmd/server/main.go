package main

import (
	"log"

	"github.com/phucle996/fps-anticheat/apps/ml-platform/go-api/internal/client"
	"github.com/phucle996/fps-anticheat/apps/ml-platform/go-api/internal/config"
	"github.com/phucle996/fps-anticheat/apps/ml-platform/go-api/internal/router"
	"github.com/phucle996/fps-anticheat/apps/ml-platform/go-api/internal/service"
)

func main() {
	log.Println("Bắt đầu khởi chạy Dịch vụ Go API Gateway...")

	// 1. Nạp cấu hình từ biến môi trường với cơ chế Fail-Close 100%
	cfg, err := config.FromEnv()
	if err != nil {
		log.Fatalf("[FAIL-CLOSE] Không thể khởi chạy Go API Gateway: %v", err)
	}

	// 2. Khởi tạo IPC Client kết nối Unix Domain Socket với Rust Engine
	ipcClient := client.NewIPCClient(cfg.IPCSocketPath)

	// 3. Khởi tạo Service Container (Business Logic Layer)
	services := service.NewServiceContainer(cfg, ipcClient)

	// 4. Khởi tạo Gin Web Framework Engine và gắn routes
	r := router.SetupRouter(cfg, services)

	addr := ":" + cfg.HTTPPort
	log.Printf("Go API Gateway đã khởi động thành công và sẵn sàng lắng nghe tại %s (Socket UDS IPC: %s)", addr, cfg.IPCSocketPath)

	if err := r.Run(addr); err != nil {
		log.Fatalf("Lỗi lắng nghe Gin HTTP Server: %v", err)
	}
}
