package parser

import (
	"errors"
)

// ErrEOF báo hiệu đã đọc hết dữ liệu trong CSV stream
var ErrEOF = errors.New("đã đọc tới cuối file (EOF)")

// ErrMalformedRow báo hiệu một dòng CSV bị sai lệch cấu trúc cột
var ErrMalformedRow = errors.New("dòng CSV bị sai lệch định dạng/số lượng cột")

// Parser định nghĩa interface đọc bản ghi thô từ nguồn dataset
type Parser interface {
	// Next đọc và trả về bản ghi tiếp theo trong stream
	Next() (*RawRecord, error)
	// Close đóng luồng đọc dữ liệu nếu cần
	Close() error
}

// RawRecord lưu trữ dữ liệu thô chưa chuẩn hóa từ một dòng trong dataset
type RawRecord struct {
	SourceFile  string            // Tên file nguồn (vd: train_V2.csv)
	RecordIndex int64             // Chỉ số dòng trong file (bắt đầu từ 1 sau Header)
	Fields      map[string]string // Map lưu tên cột -> giá trị chuỗi thô
}
