package dataset

import (
	"crypto/sha256"
	"encoding/hex"
	"fmt"
	"io"
)

// CalculateSHA256Stream tính toán chuỗi Hash SHA-256 của luồng dữ liệu mà không cần đọc hết vào RAM
func CalculateSHA256Stream(reader io.Reader) (string, error) {
	// Khởi tạo SHA-256 Hasher
	hasher := sha256.New()

	// Copy luồng dữ liệu sang Hasher bằng io.Copy buffer
	if _, err := io.Copy(hasher, reader); err != nil {
		return "", fmt.Errorf("lỗi khi tính toán SHA-256: %w", err)
	}

	// Chuyển kết quả Hex byte hash thành string
	hashBytes := hasher.Sum(nil)
	return hex.EncodeToString(hashBytes), nil
}
