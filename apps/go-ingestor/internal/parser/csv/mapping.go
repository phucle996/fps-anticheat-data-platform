package csv

import (
	"strings"
)

// ColumnMap lưu trữ bản đồ ánh xạ từ tên cột CSV sang vị trí chỉ số (Index) trong mảng
type ColumnMap map[string]int

// NewColumnMap tạo bản đồ ánh xạ động từ dòng Header của CSV
func NewColumnMap(header []string) ColumnMap {
	cMap := make(ColumnMap)
	for i, colName := range header {
		// Chuẩn hóa tên cột: xóa khoảng trắng dư thừa và chuyển về chữ thường để so sánh
		cleanName := strings.TrimSpace(colName)
		cMap[cleanName] = i
	}
	return cMap
}

// GetField lấy giá trị từ mảng dòng CSV theo tên cột dựa trên ColumnMap
func (cm ColumnMap) GetField(row []string, colName string) string {
	idx, exists := cm[colName]
	if !exists || idx < 0 || idx >= len(row) {
		return "" // Trả về chuỗi rỗng nếu cột không tồn tại hoặc vượt vị trí
	}
	return strings.TrimSpace(row[idx])
}
