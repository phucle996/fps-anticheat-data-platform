package service_test

import (
	"bytes"
	"errors"
	"os"
	"testing"

	"go-ingestor/internal/pipeline"
)

// TestCSVParser_ValidStream kiểm tra đọc thành công dòng qua dòng từ CSV hợp lệ
func TestCSVParser_ValidStream(t *testing.T) {
	file, err := os.Open("testdata/valid.csv")
	if err != nil {
		t.Fatalf("Không thể mở file testdata/valid.csv: %v", err)
	}
	defer file.Close()

	p, err := pipeline.NewCSVParser(file, "train_V2.csv")
	if err != nil {
		t.Fatalf("Khởi tạo CSVParser thất bại: %v", err)
	}
	defer p.Close()

	rec1, err := p.Next()
	if err != nil {
		t.Fatalf("Đọc bản ghi 1 thất bại: %v", err)
	}
	if rec1.RecordIndex != 1 {
		t.Errorf("Kỳ vọng RecordIndex = 1, nhận được = %d", rec1.RecordIndex)
	}
	if rec1.Fields["Id"] != "player-001" || rec1.Fields["kills"] != "5" {
		t.Errorf("Giá trị bản ghi 1 không đúng: %+v", rec1.Fields)
	}

	rec2, err := p.Next()
	if err != nil || rec2.RecordIndex != 2 {
		t.Errorf("Bản ghi 2 lỗi: %v", err)
	}

	rec3, err := p.Next()
	if err != nil || rec3.RecordIndex != 3 {
		t.Errorf("Bản ghi 3 lỗi: %v", err)
	}

	_, err = p.Next()
	if !errors.Is(err, pipeline.ErrEOF) {
		t.Errorf("Kỳ vọng trả về ErrEOF, nhận được: %v", err)
	}
}

// TestCSVParser_EmptyLineSkipping kiểm tra tính năng tự động bỏ qua dòng rỗng
func TestCSVParser_EmptyLineSkipping(t *testing.T) {
	csvData := "Id,kills\nplayer-1,3\n\n\nplayer-2,4\n"
	buf := bytes.NewBufferString(csvData)

	p, err := pipeline.NewCSVParser(buf, "test.csv")
	if err != nil {
		t.Fatalf("Khởi tạo CSVParser thất bại: %v", err)
	}

	rec1, err := p.Next()
	if err != nil || rec1.Fields["Id"] != "player-1" {
		t.Fatalf("Đọc bản ghi 1 lỗi: %v", err)
	}

	rec2, err := p.Next()
	if err != nil || rec2.Fields["Id"] != "player-2" {
		t.Fatalf("Đọc bản ghi 2 lỗi: %v", err)
	}
}
