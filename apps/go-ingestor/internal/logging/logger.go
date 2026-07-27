package logging

import (
	"os"

	"github.com/sirupsen/logrus"
)

// InitLogger khởi tạo Logrus Logger chuẩn hóa định dạng JSON theo tiêu chuẩn Cloud-Native
func InitLogger(serviceName string) *logrus.Entry {
	// 1. Cấu hình log định dạng JSONFormatter để truyền vào ELK / Grafana Loki
	logrus.SetFormatter(&logrus.JSONFormatter{
		TimestampFormat: "2006-01-02T15:04:05Z07:00", // Chuẩn ISO-8601 UTC timestamp
	})

	// 2. Xuất log ra tiêu chuẩn Standard Output (os.Stdout)
	logrus.SetOutput(os.Stdout)

	// 3. Thiết lập mức độ log mặc định là InfoLevel
	logrus.SetLevel(logrus.InfoLevel)

	// 4. Trả về Entry được đính kèm thuộc tính service cố định
	return logrus.WithField("service", serviceName)
}
