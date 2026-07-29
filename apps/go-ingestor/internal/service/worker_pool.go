package service

import (
	"context"
	"sync"
	"sync/atomic"

	"pubg-anti-cheat/go-ingestor/internal/contract"
)

// IngestionResult chứa kết quả sau khi parse & normalize 1 bản ghi CSV thô
type IngestionResult struct {
	Envelope      interface{}             // EventEnvelope (*contract.EventEnvelope hoặc *contract.KillEventEnvelope) hợp lệ
	InvalidRecord *contract.InvalidRecord // InvalidRecord vi phạm schema (nếu có)
	Err           error                   // Lỗi hệ thống nghiêm trọng
}

// IngestionWorkerPool quản lý nhóm Goroutines xử lý parse/normalize song song đa nhân CPU
type IngestionWorkerPool struct {
	workerCount int                         // Số lượng Goroutine worker song song (mặc định: runtime.NumCPU() * 2)
	rawJobCh    chan *RawRecord             // Channel đẩy bản ghi thô từ CSV Reader
	resultCh    chan *IngestionResult       // Channel nhận kết quả đã chuẩn hóa
	normalizer  Normalizer                  // Interfaced Normalizer đa schema
	wg          sync.WaitGroup              // WaitGroup đồng bộ vòng đời workers
	recordsRead atomic.Int64                // Bộ đếm atomic an toàn thread-safe cho số bản ghi đã đọc
	validRecs   atomic.Int64                // Bộ đếm atomic cho số bản ghi hợp lệ
	invalidRecs atomic.Int64                // Bộ đếm atomic cho số bản ghi vi phạm
}

// NewIngestionWorkerPool khởi tạo Worker Pool với kích thước channel đệm phù hợp
func NewIngestionWorkerPool(workerCount int, normalizer Normalizer) *IngestionWorkerPool {
	if workerCount <= 0 {
		workerCount = 8 // Mặc định 8 worker goroutines
	}

	return &IngestionWorkerPool{
		workerCount: workerCount,
		rawJobCh:    make(chan *RawRecord, workerCount*100),
		resultCh:    make(chan *IngestionResult, workerCount*100),
		normalizer:  normalizer,
	}
}

// Start khởi chạy nhóm Goroutine workers chạy song song
func (p *IngestionWorkerPool) Start(ctx context.Context) {
	for i := 0; i < p.workerCount; i++ {
		p.wg.Add(1)
		go func() {
			defer p.wg.Done()
			for rawRecord := range p.rawJobCh {
				// Tăng bộ đếm tổng số bản ghi bằng atomic (Zero Race Condition)
				p.recordsRead.Add(1)

				// Xử lý chuẩn hóa bản ghi thô qua Normalizer
				envelope, invalidRec, err := p.normalizer.Normalize(rawRecord)

				if invalidRec != nil {
					p.invalidRecs.Add(1)
				} else if envelope != nil {
					p.validRecs.Add(1)
				}

				select {
				case <-ctx.Done():
					return
				case p.resultCh <- &IngestionResult{
					Envelope:      envelope,
					InvalidRecord: invalidRec,
					Err:           err,
				}:
				}
			}
		}()
	}

	// Goroutine lắng nghe khi hoàn tất toàn bộ jobs thì close resultCh
	go func() {
		p.wg.Wait()
		close(p.resultCh)
	}()
}

// Submit đẩy 1 bản ghi thô vào channel để worker tiêu thụ
func (p *IngestionWorkerPool) Submit(raw *RawRecord) {
	p.rawJobCh <- raw
}

// CloseJobs đóng rawJobCh khi đã đọc tới cuối file CSV (EOF)
func (p *IngestionWorkerPool) CloseJobs() {
	close(p.rawJobCh)
}

// Results trả về read-only channel nhận kết quả
func (p *IngestionWorkerPool) Results() <-chan *IngestionResult {
	return p.resultCh
}

// Stats lấy các thống kê hiện tại với atomic read
func (p *IngestionWorkerPool) Stats() (read int64, valid int64, invalid int64) {
	return p.recordsRead.Load(), p.validRecs.Load(), p.invalidRecs.Load()
}
