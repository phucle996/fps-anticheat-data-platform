package csv

import (
	"encoding/csv"
	"fmt"
	"io"

	"pubg-anti-cheat/go-ingestor/internal/parser"
)

// CSVParser triển khai interface parser.Parser để đọc luồng CSV dòng qua dòng (Streaming)
type CSVParser struct {
	reader      *csv.Reader  // Go Standard CSV Reader
	sourceFile  string       // Tên file nguồn (vd: train_V2.csv)
	recordIndex int64        // Chỉ số đếm số dòng đã đọc
	columnMap   ColumnMap    // Map ánh xạ chỉ số tên cột
	headerRead  bool         // Cờ đánh dấu đã đọc dòng Header hay chưa
	closer      io.Closer    // Closer nếu input reader hỗ trợ đóng luồng
}

// NewCSVParser khởi tạo CSVParser với một io.Reader luồng dữ liệu (O(1) RAM footprint)
func NewCSVParser(reader io.Reader, sourceFile string) (*CSVParser, error) {
	if reader == nil {
		return nil, fmt.Errorf("reader không được phép nil")
	}

	csvReader := csv.NewReader(reader)
	// Cho phép số lượng cột linh hoạt để tự bắt lỗi malformed ở tầng ứng dụng
	csvReader.FieldsPerRecord = -1
	csvReader.LazyQuotes = true
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

// Next đọc dòng CSV tiếp theo trong luồng stream và trả về RawRecord
func (p *CSVParser) Next() (*parser.RawRecord, error) {
	// 1. Nếu chưa đọc Header, đọc dòng đầu tiên làm Header để tạo ColumnMap
	if !p.headerRead {
		headerRow, err := p.reader.Read()
		if err != nil {
			if err == io.EOF {
				return nil, parser.ErrEOF
			}
			return nil, fmt.Errorf("lỗi khi đọc Header CSV: %w", err)
		}
		p.columnMap = NewColumnMap(headerRow)
		p.headerRead = true
	}

	// 2. Vòng lặp đọc dòng dữ liệu tiếp theo (tự động bỏ qua các dòng rỗng)
	for {
		row, err := p.reader.Read()
		if err != nil {
			if err == io.EOF {
				return nil, parser.ErrEOF
			}
			return nil, fmt.Errorf("lỗi khi đọc dòng CSV: %w", err)
		}

		p.recordIndex++ // Tăng số đếm chỉ số dòng bản ghi

		// Bỏ qua dòng hoàn toàn rỗng
		if len(row) == 0 || (len(row) == 1 && row[0] == "") {
			continue
		}

		// Khởi tạo RawRecord cho dòng hiện tại
		record := NewRawRecord(p.sourceFile, p.recordIndex)

		// Gán toàn bộ các giá trị cột từ Header vào Record Fields
		for colName := range p.columnMap {
			record.Fields[colName] = p.columnMap.GetField(row, colName)
		}

		return record, nil
	}
}

// Close đóng luồng reader nguồn nếu hỗ trợ io.Closer
func (p *CSVParser) Close() error {
	if p.closer != nil {
		return p.closer.Close()
	}
	return nil
}
