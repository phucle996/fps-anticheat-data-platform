package main

import (
	"log"

	"go-api/internal/app"
)

func main() {
	// Khởi tạo ứng dụng từ app.NewApp()
	application, err := app.NewApp()
	if err != nil {
		log.Fatalf("[FATAL] Khởi tạo Go API Gateway thất bại: %v", err)
	}

	// Thực thi vòng đời chạy HTTP Server & Graceful Shutdown
	if err := application.Run(); err != nil {
		log.Fatalf("[FATAL] Lỗi trong quá trình thực thi Go API Gateway: %v", err)
	}
}
