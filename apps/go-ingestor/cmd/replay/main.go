package main

import (
	"context"
	"flag"
	"os"
	"os/signal"
	"syscall"

	"pubg-anti-cheat/go-ingestor/internal/app"
	"pubg-anti-cheat/go-ingestor/internal/config"
	"pubg-anti-cheat/go-ingestor/internal/logging"
	"pubg-anti-cheat/go-ingestor/internal/replay"
)

func main() {
	// 1. Định nghĩa CLI Flags điều khiển replay
	limitFlag := flag.Int64("limit", 0, "Số bản ghi tối đa cần replay (0 = không giới hạn)")
	startFlag := flag.Int64("start", 1, "Chỉ số bản ghi bắt đầu replay (1-indexed)")
	dryRunFlag := flag.Bool("dry-run", true, "Chế độ chạy thử không đẩy tin nhắn vào Kafka")
	flag.Parse()

	// 2. Khởi tạo Logrus JSON Logger cho dịch vụ
	log := logging.InitLogger("go-replay")
	log.Info("Khởi động tiến trình replay entrypoint...")

	// 3. Bắt tín hiệu Hủy / Graceful Shutdown từ OS (SIGINT, SIGTERM)
	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()

	// 4. Nạp cấu hình ứng dụng từ Environment Variables
	cfg, err := config.LoadFromEnv()
	if err != nil {
		log.WithError(err).Fatal("Nạp cấu hình ứng dụng thất bại")
	}

	// 5. Khởi tạo ReplayApp với các dependencies
	replayApp, err := app.NewReplayApp(cfg, log)
	if err != nil {
		log.WithError(err).Fatal("Khởi tạo ứng dụng ReplayApp thất bại")
	}

	// 6. Cấu hình các tham số Replay
	replayConfig := replay.ReplayerConfig{
		Limit:       *limitFlag,
		StartRecord: *startFlag,
		DryRun:      *dryRunFlag,
	}

	// 7. Thực thi Use Case Replay Dataset
	if _, err := replayApp.Run(ctx, replayConfig); err != nil {
		if ctx.Err() != nil {
			log.Warn("Tiến trình replay bị ngắt bởi tín hiệu Graceful Shutdown từ hệ thống")
		} else {
			log.WithError(err).Fatal("Thực thi tiến trình replay thất bại")
		}
	}

	log.Info("Tiến trình replay hoàn thành và kết thúc an toàn.")
}
