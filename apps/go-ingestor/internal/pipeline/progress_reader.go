package pipeline

import (
	"fmt"
	"io"
	"os"
	"strings"
	"time"
)

// ProgressReader bọc một io.Reader để theo dõi dung lượng dữ liệu đang đọc và hiển thị live Progress Bar (%) trên Terminal
// Phục vụ hiển thị tiến trình tải dataset từ Kaggle hoặc đọc file lớn trong môi trường CLI
type ProgressReader struct {
	reader     io.Reader // Luồng đọc nguồn (HTTP Response Body hoặc File Stream)
	totalBytes int64     // Tổng dung lượng dữ liệu (bytes) nếu xác định được Content-Length
	readBytes  int64     // Tích lũy số bytes đã đọc được
	startTime  time.Time // Thời điểm bắt đầu đọc
	lastPrint  time.Time // Thời điểm cập nhật giao diện lần gần nhất
	taskName   string    // Tên công việc hiển thị (ví dụ: "Kaggle Dataset Download")
}

// NewProgressReader khởi tạo ProgressReader với tổng dung lượng bytes và tên task
func NewProgressReader(reader io.Reader, totalBytes int64, taskName string) *ProgressReader {
	return &ProgressReader{
		reader:     reader,
		totalBytes: totalBytes,
		startTime:  time.Now(),
		lastPrint:  time.Now(),
		taskName:   taskName,
	}
}

// Read thực thi đọc từng chunk buffer từ reader nguồn và cập nhật tiến trình
func (pr *ProgressReader) Read(p []byte) (int, error) {
	n, err := pr.reader.Read(p)
	if n > 0 {
		pr.readBytes += int64(n)
		now := time.Now()

		// Giới hạn tần suất in tiến trình mỗi 100ms hoặc khi đọc tới EOF để tránh gây giật lag Terminal UI
		if now.Sub(pr.lastPrint) >= 100*time.Millisecond || err == io.EOF || (pr.totalBytes > 0 && pr.readBytes >= pr.totalBytes) {
			pr.lastPrint = now
			pr.render()
		}
	}
	if err == io.EOF {
		fmt.Println() // Đổi dòng mới khi hoàn thành tải toàn bộ luồng
	}
	return n, err
}

// render vẽ thanh tiến trình ANSI trên cùng 1 dòng bằng ký tự điều khiển escape '\r'
func (pr *ProgressReader) render() {
	elapsed := time.Since(pr.startTime).Seconds()
	if elapsed <= 0 {
		elapsed = 0.001
	}

	// Tốc độ truyền tải dữ liệu tính theo MB/s
	speedMBps := (float64(pr.readBytes) / (1024 * 1024)) / elapsed
	readMB := float64(pr.readBytes) / (1024 * 1024)

	if pr.totalBytes > 0 {
		totalMB := float64(pr.totalBytes) / (1024 * 1024)
		percent := (float64(pr.readBytes) / float64(pr.totalBytes)) * 100.0
		if percent > 100.0 {
			percent = 100.0
		}

		// Tính toán thời gian còn lại dự kiến (ETA)
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

		// Định dạng Terminal UI chuyên nghiệp
		fmt.Printf("\r[+] %s: [%s] %5.1f%% (%6.1f/%6.1f MB) | %5.1f MB/s | ETA: %2ds   ",
			pr.taskName, bar, percent, readMB, totalMB, speedMBps, etaSec)
	} else {
		fmt.Printf("\r[+] %s: %6.1f MB downloaded | %5.1f MB/s   ",
			pr.taskName, readMB, speedMBps)
	}
	_ = os.Stdout.Sync()
}
