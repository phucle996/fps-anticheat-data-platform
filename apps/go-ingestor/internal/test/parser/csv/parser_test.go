package csv_test

import (
	"bytes"
	"errors"
	"os"
	"testing"

	"pubg-anti-cheat/go-ingestor/internal/parser"
	"pubg-anti-cheat/go-ingestor/internal/parser/csv"
)

// TestCSVParser_ValidStream kiểm tra đọc thành công dòng qua dòng từ CSV hợp lệ trong internal/test/parser/csv/
func TestCSVParser_ValidStream(t *testing.T) {
	// 1. Mở file testdata/valid.csv nằm trong internal/test/parser/csv/testdata/
	file, err := os.Open("testdata/valid.csv")
	if err != nil {
		t.Fatalf("Không thể mở file testdata/valid.csv: %v", err)
	}
	defer file.Close()

	// 2. Khởi tạo CSVParser với luồng stream file
	p, err := csv.NewCSVParser(file, "train_V2.csv")
	if err != nil {
		t.Fatalf("Khởi tạo CSVParser thất bại: %v", err)
	}
	defer p.Close()

	// 3. Đọc bản ghi thứ 1
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

	// 4. Đọc bản ghi thứ 2
	rec2, err := p.Next()
	if err != nil {
		t.Fatalf("Đọc bản ghi 2 thất bại: %v", err)
	}
	if rec2.RecordIndex != 2 {
		t.Errorf("Kỳ vọng RecordIndex = 2, nhận được = %d", rec2.RecordIndex)
	}

	// 5. Đọc bản ghi thứ 3
	rec3, err := p.Next()
	if err != nil {
		t.Fatalf("Đọc bản ghi 3 thất bại: %v", err)
	}
	if rec3.RecordIndex != 3 {
		t.Errorf("Kỳ vọng RecordIndex = 3, nhận được = %d", rec3.RecordIndex)
	}

	// 6. Đọc tiếp theo phải trả về ErrEOF
	_, err = p.Next()
	if !errors.Is(err, parser.ErrEOF) {
		t.Errorf("Kỳ vọng trả về ErrEOF, nhận được: %v", err)
	}
}

// TestCSVParser_EmptyLineSkipping kiểm tra tính năng tự động bỏ qua dòng rỗng
func TestCSVParser_EmptyLineSkipping(t *testing.T) {
	// Dữ liệu CSV giả lập chứa các dòng rỗng ở giữa
	csvData := "Id,kills\nplayer-1,3\n\n\nplayer-2,4\n"
	buf := bytes.NewBufferString(csvData)

	// Khởi tạo CSVParser từ Buffer stream
	p, err := csv.NewCSVParser(buf, "test.csv")
	if err != nil {
		t.Fatalf("Khởi tạo CSVParser thất bại: %v", err)
	}

	// Bản ghi 1 đọc player-1
	rec1, err := p.Next()
	if err != nil || rec1.Fields["Id"] != "player-1" {
		t.Fatalf("Đọc bản ghi 1 lỗi: %v", err)
	}

	// Bản ghi 2 đọc player-2 bỏ qua 2 dòng rỗng ở giữa
	rec2, err := p.Next()
	if err != nil || rec2.Fields["Id"] != "player-2" {
		t.Fatalf("Đọc bản ghi 2 lỗi: %v", err)
	}
}
