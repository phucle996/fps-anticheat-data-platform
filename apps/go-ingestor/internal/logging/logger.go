package logging

import (
	"log/slog"
	"os"
)

// InitLogger khởi tạo logger chuẩn hóa định dạng JSON theo tiêu chuẩn Cloud-Native
func InitLogger(serviceName string) *slog.Logger {
	// Sử dụng JSONHandler để log có cấu trúc dễ truyền vào ELK / Grafana Loki
	handler := slog.NewJSONHandler(os.Stdout, &slog.HandlerOptions{
		Level: slog.LevelInfo, // Mức log mặc định là INFO
	})

	// Gắn sẵn thuộc tính cố định service vào mọi log entry
	logger := slog.New(handler).With(
		slog.String("service", serviceName),
	)

	// Đặt logger mặc định cho ứng dụng
	slog.SetDefault(logger)
	return logger
}
