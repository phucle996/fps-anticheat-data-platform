package pipeline

import (
	"encoding/csv"
	"errors"
	"fmt"
	"io"
	"strings"
)

// ErrEOF báo hiệu đã đọc tới cuối luồng dữ liệu trong CSV stream (Zero Data Remaining)
var ErrEOF = errors.New("đã đọc tới cuối file (EOF)")

// ErrMalformedRow báo hiệu một dòng trong CSV bị sai lệch cấu trúc cột/định dạng
var ErrMalformedRow = errors.New("dòng CSV bị sai lệch định dạng/số lượng cột")

// Parser định nghĩa hợp đồng interface đọc bản ghi thô từ nguồn Dataset
// Thiết kế theo hướng Abstraction để dễ dàng hỗ trợ các loại Parser khác (JSON, Parquet, Stream Reader)
type Parser interface {
	// Next đọc và trả về bản ghi thô tiếp theo trong luồng (Stream)
	Next() (*RawRecord, error)
	// Close giải phóng tài nguyên luồng reader bên dưới
	Close() error
}

// RawRecord lưu trữ dữ liệu thô chưa qua xử lý từ một dòng trong dataset CSV
type RawRecord struct {
	SourceFile  string            // Tên file nguồn (ví dụ: kill_match_stats_final_0.csv hoặc train_V2.csv)
	RecordIndex int64             // Chỉ số dòng trong file (bắt đầu từ 1 sau dòng Header)
	Fields      map[string]string // Map lưu ánh xạ Tên cột -> Giá trị chuỗi thô
}

// NewRawRecord khởi tạo đối tượng RawRecord mới với map Fields rỗng
func NewRawRecord(sourceFile string, recordIndex int64) *RawRecord {
	return &RawRecord{
		SourceFile:  sourceFile,
		RecordIndex: recordIndex,
		Fields:      make(map[string]string),
	}
}

// ColumnMap lưu trữ bản đồ ánh xạ từ tên cột CSV sang vị trí index (0-based) trong mảng
type ColumnMap map[string]int

// NewColumnMap tạo bản đồ ánh xạ động dựa trên danh sách tên cột trong dòng Header của file CSV
func NewColumnMap(header []string) ColumnMap {
	cMap := make(ColumnMap)
	for i, colName := range header {
		cleanName := strings.TrimSpace(colName)
		cMap[cleanName] = i
	}
	return cMap
}

// GetField truy xuất an toàn giá trị cột theo tên từ mảng chuỗi row, chống lỗi Out of Bounds Index
func (cm ColumnMap) GetField(row []string, colName string) string {
	idx, exists := cm[colName]
	if !exists || idx < 0 || idx >= len(row) {
		return ""
	}
	return strings.TrimSpace(row[idx])
}

// CSVParser triển khai interface Parser để đọc luồng dữ liệu CSV dòng qua dòng (Zero-RAM Streaming)
// Đảm bảo không nạp toàn bộ file CSV hàng GB vào bộ nhớ RAM
type CSVParser struct {
	reader      *csv.Reader // Go Standard Library CSV Reader
	sourceFile  string      // Tên file nguồn đang xử lý
	recordIndex int64       // Bộ đếm đếm số bản ghi hợp lệ đã đọc
	columnMap   ColumnMap   // Bản đồ ánh xạ cột
	headerRead  bool        // Cờ trạng thái đánh dấu dòng Header đã được đọc hay chưa
	closer      io.Closer   // Interface Closer để thu hồi tài nguyên luồng
}

// NewCSVParser khởi tạo CSVParser với cấu hình lỏng để xử lý dữ liệu PUBG Telemetry thực tế
func NewCSVParser(reader io.Reader, sourceFile string) (*CSVParser, error) {
	csvReader := csv.NewReader(reader)
	csvReader.FieldsPerRecord = -1 // Cho phép số lượng cột linh hoạt giữa các dòng
	csvReader.LazyQuotes = true      // Cho phép dấu ngoặc kép không chuẩn trong dữ liệu text
	csvReader.TrimLeadingSpace = true

	var closer io.Closer
	if c, ok := reader.(io.Closer); ok {
		closer = c
	}

	return &CSVParser{
		reader:      csvReader,
		sourceFile:  sourceFile,
		recordIndex: 0,
		columnMap:   make(ColumnMap),
		headerRead:  false,
		closer:      closer,
	}, nil
}

// Next đọc dòng CSV tiếp theo trong luồng stream và trả về RawRecord đã được map cột
func (p *CSVParser) Next() (*RawRecord, error) {
	// Nếu chưa đọc Header, đọc dòng đầu tiên để xây dựng ColumnMap
	if !p.headerRead {
		headerRow, err := p.reader.Read()
		if err != nil {
			if err == io.EOF {
				return nil, ErrEOF
			}
			return nil, fmt.Errorf("lỗi khi đọc Header CSV từ %s: %w", p.sourceFile, err)
		}
		p.columnMap = NewColumnMap(headerRow)
		p.headerRead = true
	}

	for {
		row, err := p.reader.Read()
		if err != nil {
			if err == io.EOF {
				return nil, ErrEOF
			}
			return nil, fmt.Errorf("lỗi khi đọc dòng CSV thứ %d: %w", p.recordIndex+1, err)
		}

		p.recordIndex++

		// Bỏ qua các dòng rỗng hoặc dòng chứa khoảnh trắng rỗng
		if len(row) == 0 || (len(row) == 1 && strings.TrimSpace(row[0]) == "") {
			continue
		}

		record := NewRawRecord(p.sourceFile, p.recordIndex)
		for colName := range p.columnMap {
			record.Fields[colName] = p.columnMap.GetField(row, colName)
		}

		return record, nil
	}
}

// Close đóng luồng reader nguồn an toàn
func (p *CSVParser) Close() error {
	if p.closer != nil {
		return p.closer.Close()
	}
	return nil
}

// Ensure interface compliance at compile-time
var _ Parser = (*CSVParser)(nil)
