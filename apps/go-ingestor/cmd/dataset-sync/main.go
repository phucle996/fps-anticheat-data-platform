package main

import (
	"context"
	"os"
	"os/signal"
	"syscall"

	"pubg-anti-cheat/go-ingestor/internal/app"
	"pubg-anti-cheat/go-ingestor/internal/config"
	"pubg-anti-cheat/go-ingestor/internal/logging"
)

func main() {
	// 1. Khởi tạo Logrus JSON Logger cho dịch vụ
	log := logging.InitLogger("go-ingestor")
	log.Info("Khởi tạo tiến trình dataset-sync entrypoint...")

	// 2. Khởi tạo Context bắt tín hiệu Hủy / Graceful Shutdown từ OS (SIGINT, SIGTERM)
	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()

	// 3. Nạp cấu hình ứng dụng từ Environment Variables
	cfg, err := config.LoadFromEnv()
	if err != nil {
		log.WithError(err).Fatal("Nạp cấu hình ứng dụng thất bại")
	}

	// 4. Khởi tạo đối tượng App với các dependencies đã nạp
	datasetApp, err := app.NewDatasetSyncApp(cfg, log)
	if err != nil {
		log.WithError(err).Fatal("Khởi tạo ứng dụng DatasetSyncApp thất bại")
	}

	// 5. Thực thi Use Case ứng dụng và xử lý lỗi
	if err := datasetApp.Run(ctx); err != nil {
		if ctx.Err() != nil {
			log.Warn("Tiến trình bị ngắt bởi tín hiệu Graceful Shutdown từ hệ thống")
		} else {
			log.WithError(err).Fatal("Thực thi tiến trình dataset-sync thất bại")
		}
	}

	log.Info("Tiến trình dataset-sync hoàn thành và kết thúc an toàn.")
}
