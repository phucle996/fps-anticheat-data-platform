package service

import (
	"encoding/csv"
	"errors"
	"fmt"
	"io"
	"strings"
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

// NewRawRecord tạo một đối tượng RawRecord mới với map khởi tạo sẵn
func NewRawRecord(sourceFile string, recordIndex int64) *RawRecord {
	return &RawRecord{
		SourceFile:  sourceFile,
		RecordIndex: recordIndex,
		Fields:      make(map[string]string),
	}
}

// ColumnMap lưu trữ bản đồ ánh xạ từ tên cột CSV sang vị trí chỉ số (Index) trong mảng
type ColumnMap map[string]int

// NewColumnMap tạo bản đồ ánh xạ động từ dòng Header của CSV
func NewColumnMap(header []string) ColumnMap {
	cMap := make(ColumnMap)
	for i, colName := range header {
		cleanName := strings.TrimSpace(colName)
		cMap[cleanName] = i
	}
	return cMap
}

// GetField lấy giá trị từ mảng dòng CSV theo tên cột dựa trên ColumnMap
func (cm ColumnMap) GetField(row []string, colName string) string {
	idx, exists := cm[colName]
	if !exists || idx < 0 || idx >= len(row) {
		return ""
	}
	return strings.TrimSpace(row[idx])
}

// CSVParser triển khai interface Parser để đọc luồng CSV dòng qua dòng (Streaming)
type CSVParser struct {
	reader      *csv.Reader // Go Standard CSV Reader
	sourceFile  string      // Tên file nguồn (vd: train_V2.csv)
	recordIndex int64       // Chỉ số đếm số dòng đã đọc
	columnMap   ColumnMap   // Map ánh xạ chỉ số tên cột
	headerRead  bool        // Cờ đánh dấu đã đọc dòng Header hay chưa
	closer      io.Closer   // Closer nếu input reader hỗ trợ đóng luồng
}

// NewCSVParser khởi tạo CSVParser từ io.Reader stream (Tin tưởng tham số đã được Thượng nguồn cung cấp)
func NewCSVParser(reader io.Reader, sourceFile string) (*CSVParser, error) {
	csvReader := csv.NewReader(reader)
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
func (p *CSVParser) Next() (*RawRecord, error) {
	if !p.headerRead {
		headerRow, err := p.reader.Read()
		if err != nil {
			if err == io.EOF {
				return nil, ErrEOF
			}
			return nil, fmt.Errorf("lỗi khi đọc Header CSV: %w", err)
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
			return nil, fmt.Errorf("lỗi khi đọc dòng CSV: %w", err)
		}

		p.recordIndex++

		// Bỏ qua các dòng rỗng
		if len(row) == 0 || (len(row) == 1 && row[0] == "") {
			continue
		}

		record := NewRawRecord(p.sourceFile, p.recordIndex)
		for colName := range p.columnMap {
			record.Fields[colName] = p.columnMap.GetField(row, colName)
		}

		return record, nil
	}
}

// Close đóng luồng reader nguồn nếu có
func (p *CSVParser) Close() error {
	if p.closer != nil {
		return p.closer.Close()
	}
	return nil
}
