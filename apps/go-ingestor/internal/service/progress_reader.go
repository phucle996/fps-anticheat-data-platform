package service

import (
	"fmt"
	"io"
	"os"
	"strings"
	"time"
)

// ProgressReader bọc io.Reader để theo dõi dung lượng và hiển thị live Progress Bar phần trăm (%) trên Terminal
type ProgressReader struct {
	reader     io.Reader // Stream nguồn (HTTP Body hoặc File)
	totalBytes int64     // Tổng dung lượng file (bytes)
	readBytes  int64     // Dung lượng đã tải tích lũy (bytes)
	startTime  time.Time // Thời điểm bắt đầu tải
	lastPrint  time.Time // Thời điểm in tiến trình lần gần nhất
	taskName   string    // Tên công việc hiển thị (vd: "Kaggle Dataset Download")
}

// NewProgressReader khởi tạo ProgressReader với tổng dung lượng file
func NewProgressReader(reader io.Reader, totalBytes int64, taskName string) *ProgressReader {
	return &ProgressReader{
		reader:     reader,
		totalBytes: totalBytes,
		startTime:  time.Now(),
		lastPrint:  time.Now(),
		taskName:   taskName,
	}
}

// Read thực thi đọc từng chunk buffer và cập nhật giao diện Progress Bar
func (pr *ProgressReader) Read(p []byte) (int, error) {
	n, err := pr.reader.Read(p)
	if n > 0 {
		pr.readBytes += int64(n)
		now := time.Now()

		// Giới hạn tần suất in tiến trình mỗi 100ms hoặc khi kết thúc để tránh giật lag Terminal
		if now.Sub(pr.lastPrint) >= 100*time.Millisecond || err == io.EOF || (pr.totalBytes > 0 && pr.readBytes >= pr.totalBytes) {
			pr.lastPrint = now
			pr.render()
		}
	}
	if err == io.EOF {
		fmt.Println() // Đổ dòng mới khi hoàn thành tải dữ liệu
	}
	return n, err
}

// render vẽ thanh tiến trình ANSI trên cùng 1 dòng bằng ký tự '\r'
func (pr *ProgressReader) render() {
	elapsed := time.Since(pr.startTime).Seconds()
	if elapsed <= 0 {
		elapsed = 0.001
	}

	// Tốc độ tải tính theo MB/s
	speedMBps := (float64(pr.readBytes) / (1024 * 1024)) / elapsed
	readMB := float64(pr.readBytes) / (1024 * 1024)

	if pr.totalBytes > 0 {
		totalMB := float64(pr.totalBytes) / (1024 * 1024)
		percent := (float64(pr.readBytes) / float64(pr.totalBytes)) * 100.0
		if percent > 100.0 {
			percent = 100.0
		}

		// Thời gian ước tính còn lại (ETA)
		remainingBytes := pr.totalBytes - pr.readBytes
		etaSec := int64(0)
		if speedMBps > 0 {
			etaSec = int64((float64(remainingBytes) / (1024 * 1024)) / speedMBps)
		}

		width := 25
		filled := int((percent / 100.0) * float64(width))
		if filled > width {
			filled = width
		}
		bar := strings.Repeat("=", filled)
		if filled < width {
			bar += ">" + strings.Repeat("-", width-filled-1)
		}

		// Format chuỗi Terminal UI chuyên nghiệp
		fmt.Printf("\r[+] %s: [%s] %5.1f%% (%6.1f/%6.1f MB) | %5.1f MB/s | ETA: %2ds   ",
			pr.taskName, bar, percent, readMB, totalMB, speedMBps, etaSec)
	} else {
		fmt.Printf("\r[+] %s: %6.1f MB downloaded | %5.1f MB/s   ",
			pr.taskName, readMB, speedMBps)
	}
	os.Stdout.Sync()
}
