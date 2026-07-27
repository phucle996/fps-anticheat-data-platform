package csv

import (
	"pubg-anti-cheat/go-ingestor/internal/parser"
)

// NewRawRecord tạo một đối tượng RawRecord mới với map khởi tạo sẵn
func NewRawRecord(sourceFile string, recordIndex int64) *parser.RawRecord {
	return &parser.RawRecord{
		SourceFile:  sourceFile,
		RecordIndex: recordIndex,
		Fields:      make(map[string]string),
	}
}
