package main

import (
	"context"
	"flag"
	"os"
	"os/signal"
	"syscall"

	"go-ingestor/internal/config"
	"go-ingestor/internal/logging"
	"go-ingestor/internal/service"
)

func main() {
	// 1. Định nghĩa CLI Flags điều khiển replay, stream delay và checkpointing
	limitFlag := flag.Int64("limit", 0, "Số bản ghi tối đa cần replay (0 = không giới hạn)")
	startFlag := flag.Int64("start", 1, "Chỉ số bản ghi bắt đầu replay (1-indexed)")
	dryRunFlag := flag.Bool("dry-run", true, "Chế độ chạy thử không đẩy tin nhắn vào Kafka")
	disableCpFlag := flag.Bool("disable-checkpoint", false, "Tắt tính năng nạp/lưu Checkpoint trên MinIO S3")
	resetCpFlag := flag.Bool("reset-checkpoint", false, "Xóa trạng thái Checkpoint cũ trên MinIO S3 trước khi bắt đầu")
	streamDelayMsFlag := flag.Int64("stream-delay-ms", 0, "Khoảng trễ (ms) phát rải rác giữa các bản ghi game events (0 = xả tốc độ tối đa)")
	flag.Parse()

	// 2. Khởi tạo Logrus JSON Logger cho dịch vụ
	log := logging.InitLogger("go-replay")
	log.Info("Khởi động tiến trình replay entrypoint (Checkpoint & Stream Simulator Ready)...")

	// 3. Bắt tín hiệu Hủy / Graceful Shutdown từ OS (SIGINT, SIGTERM)
	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()

	// 4. Nạp cấu hình ứng dụng từ Environment Variables (Fail-Close)
	cfg, err := config.LoadFromEnv()
	if err != nil {
		log.WithError(err).Fatal("Nạp cấu hình ứng dụng thất bại")
	}

	// 5. Khởi tạo MinIO Client và ReplayService
	minioCli, err := service.NewMinIOClient(cfg.MinIOEndpoint, cfg.MinIOAccessKey, cfg.MinIOSecretKey, cfg.MinIOBucket, cfg.MinIOUseSSL)
	if err != nil {
		log.WithError(err).Fatal("Khởi tạo MinIOClient thất bại")
	}

	replayService := service.NewReplayService(cfg, minioCli, log)

	// 6. Cấu hình các tham số Replay, Stream Simulator Delay và Checkpoint
	replayConfig := service.ReplayerConfig{
		Limit:             *limitFlag,
		StartRecord:       *startFlag,
		DryRun:            *dryRunFlag,
		DisableCheckpoint: *disableCpFlag,
		ResetCheckpoint:   *resetCpFlag,
		StreamDelayMs:     *streamDelayMsFlag,
	}

	// 7. Thực thi Use Case Replay Dataset
	if _, err := replayService.RunReplay(ctx, replayConfig); err != nil {
		if ctx.Err() != nil {
			log.Info("🟢 Tiến trình replay đã dừng an toàn theo yêu cầu ngắt từ người dùng (Graceful Shutdown - Exit Code 0).")
		} else {
			log.WithError(err).Fatal("Thực thi tiến trình replay thất bại")
		}
	} else if ctx.Err() != nil {
		log.Info("🟢 Tiến trình replay đã dừng an toàn theo yêu cầu ngắt từ người dùng (Graceful Shutdown - Exit Code 0).")
	} else {
		log.Info("Tiến trình replay hoàn thành và kết thúc an toàn.")
	}
}
