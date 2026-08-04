package app

import (
	"context"
	"errors"
	"fmt"
	"log"
	"net/http"
	"os"
	"os/signal"
	"syscall"
	"time"

	"github.com/gin-gonic/gin"
)

// App đại diện cho toàn bộ ứng dụng Go API Gateway
// Quản lý vòng đời khởi tạo (Bootstrap) qua Module, Router và Graceful Shutdown
type App struct {
	module *Module
	router *gin.Engine
}

// NewApp khởi tạo và tự động wire tất cả các thành phần ứng dụng thông qua Module tập trung
func NewApp() (*App, error) {
	log.Println("⚡ [BOOTSTRAP] Khởi động tiến trình Bootstrap cho Dịch vụ Go API Gateway...")

	// 1. Khởi tạo tập trung tất cả các constructors (Config, Infra Clients, Services, Handlers) qua Module
	mod, err := NewModule()
	if err != nil {
		return nil, fmt.Errorf("khởi tạo Module thất bại: %w", err)
	}

	// 2. Khởi tạo Gin Web Framework Engine và gắn routes từ Module
	log.Println("🌐 [ROUTER] Khởi tạo Gin Framework Router & API Endpoints...")
	r := SetupRouter(mod)

	return &App{
		module: mod,
		router: r,
	}, nil
}

// Run khởi chạy HTTP Web Server và xử lý Graceful Shutdown khi nhận tín hiệu từ hệ điều hành (SIGINT, SIGTERM)
func (a *App) Run() error {
	addr := ":" + a.module.Cfg.HTTPPort
	log.Printf("🚀 [READY] Go API Gateway đã khởi động thành công và sẵn sàng phục vụ tại %s (IPC Socket: %s)", addr, a.module.Cfg.IPCSocketPath)

	server := &http.Server{
		Addr:    addr,
		Handler: a.router,
	}

	// Goroutine lắng nghe tín hiệu OS ngắt tiến trình (Ctrl+C, K8s SIGTERM)
	shutdownErrCh := make(chan error, 1)
	go func() {
		quit := make(chan os.Signal, 1)
		signal.Notify(quit, syscall.SIGINT, syscall.SIGTERM)
		sig := <-quit
		log.Printf("🛑 [SHUTDOWN] Nhận được tín hiệu %v từ OS. Bắt đầu tiến trình Graceful Shutdown...", sig)

		ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
		defer cancel()

		if err := server.Shutdown(ctx); err != nil {
			shutdownErrCh <- fmt.Errorf("lỗi Graceful Shutdown HTTP Server: %w", err)
			return
		}
		shutdownErrCh <- nil
	}()

	// Chạy HTTP Server
	if err := server.ListenAndServe(); err != nil && !errors.Is(err, http.ErrServerClosed) {
		return fmt.Errorf("lỗi lắng nghe HTTP Server: %w", err)
	}

	// Đợi hoàn tất Graceful Shutdown
	err := <-shutdownErrCh
	if err == nil {
		log.Println("🟢 [EXIT] Go API Gateway đã dừng an toàn và giải phóng toàn bộ tài nguyên (Exit Code 0).")
	}
	return err
}
