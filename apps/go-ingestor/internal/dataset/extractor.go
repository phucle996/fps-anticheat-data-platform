package dataset

import (
	"archive/zip"
	"bytes"
	"fmt"
	"io"
	"path/filepath"
	"strings"
)

// ExtractedFile chứa thông tin file đã được trích xuất từ archive Zip
type ExtractedFile struct {
	Name    string    // Tên file (ví dụ: train_V2.csv)
	Content io.Reader // Reader chứa nội dung dữ liệu file
	Size    int64     // Dung lượng file (bytes)
}

// ExtractZipFileFromBuffer đọc và tìm file CSV chỉ định trong Zip Buffer an toàn
func ExtractZipFileFromBuffer(zipBytes []byte, targetFileName string) (*ExtractedFile, error) {
	// Khởi tạo Reader cho Zip byte buffer
	bytesReader := bytes.NewReader(zipBytes)
	zipReader, err := zip.NewReader(bytesReader, int64(len(zipBytes)))
	if err != nil {
		return nil, fmt.Errorf("không thể đọc định dạng Zip Archive: %w", err)
	}

	// Duyệt qua danh sách các file trong Zip
	for _, file := range zipReader.File {
		// Bảo mật: Kiểm tra Zip Slip vulnerability
		cleanPath := filepath.Clean(file.Name)
		if strings.HasPrefix(cleanPath, "..") || strings.HasPrefix(cleanPath, "/") {
			return nil, fmt.Errorf("phát hiện đường dẫn không an toàn trong Zip (Zip Slip Attack): %s", file.Name)
		}

		// Nếu trùng tên với target CSV file cần tìm (ví dụ train_V2.csv hoặc filepath kết thúc bằng targetFileName)
		if file.Name == targetFileName || filepath.Base(file.Name) == targetFileName {
			rc, err := file.Open()
			if err != nil {
				return nil, fmt.Errorf("không thể mở file '%s' trong Zip: %w", file.Name, err)
			}

			// Trả về đối tượng ExtractedFile
			return &ExtractedFile{
				Name:    filepath.Base(file.Name),
				Content: rc,
				Size:    int64(file.UncompressedSize64),
			}, nil
		}
	}

	return nil, fmt.Errorf("không tìm thấy file '%s' trong Zip Archive", targetFileName)
}
