package normalize

import (
	"pubg-anti-cheat/go-ingestor/internal/contract"
	"pubg-anti-cheat/go-ingestor/internal/parser"
)

// Normalizer định nghĩa interface chuẩn hóa bản ghi thô thành EventEnvelope hoặc InvalidRecord
type Normalizer interface {
	// Normalize thực hiện parse, validate và đóng gói EventEnvelope hoặc InvalidRecord
	Normalize(raw *parser.RawRecord) (*contract.EventEnvelope, *contract.InvalidRecord, error)
}
