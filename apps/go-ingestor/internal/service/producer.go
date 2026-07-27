package service

import (
	"context"
	"encoding/json"
	"fmt"
	"time"

	"github.com/segmentio/kafka-go"
	"github.com/segmentio/kafka-go/compress"
	"github.com/sirupsen/logrus"

	"pubg-anti-cheat/go-ingestor/internal/contract"
)

// Producer định nghĩa interface phát tin nhắn vào Kafka Cluster (HA & Fail-Close)
type Producer interface {
	// ProduceEvent phát EventEnvelope hợp lệ vào Raw Topic (Key = match_id)
	ProduceEvent(ctx context.Context, envelope *contract.EventEnvelope) error
	// ProduceInvalid phát InvalidRecord vi phạm vào DLQ Topic (Key = source_file)
	ProduceInvalid(ctx context.Context, invalid *contract.InvalidRecord) error
	// Close đóng các Kafka writers và flush dữ liệu an toàn
	Close() error
}

// KafkaProducer triển khai Producer interface bằng thư viện Pure Go segmentio/kafka-go
type KafkaProducer struct {
	rawWriter     *kafka.Writer // Writer phát dữ liệu hợp lệ vào Raw Topic
	invalidWriter *kafka.Writer // Writer phát dữ liệu lỗi vào DLQ Topic
	log           *logrus.Entry // Logger JSON
}

// NewKafkaProducer khởi tạo KafkaProducer với cấu hình HA (acks=all, zstd compression, hash balancer)
func NewKafkaProducer(brokers []string, rawTopic, invalidTopic string, log *logrus.Entry) (*KafkaProducer, error) {
	// Validate Fail-Close: Không cho phép nạp brokers hoặc topics rỗng
	if len(brokers) == 0 {
		return nil, fmt.Errorf("không thể khởi tạo KafkaProducer: danh sách brokers rỗng (Fail-Close)")
	}
	if rawTopic == "" {
		return nil, fmt.Errorf("không thể khởi tạo KafkaProducer: rawTopic rỗng (Fail-Close)")
	}
	if invalidTopic == "" {
		return nil, fmt.Errorf("không thể khởi tạo KafkaProducer: invalidTopic rỗng (Fail-Close)")
	}

	// 1. Cấu hình Writer cho Raw Topic (pubg.v1.player-stat.raw)
	rawWriter := &kafka.Writer{
		Addr:         kafka.TCP(brokers...),
		Topic:        rawTopic,
		Balancer:     &kafka.Hash{},                  // Partitioning theo Message Key (match_id)
		RequiredAcks: kafka.RequireAll,               // acks=all (Bắt buộc ISR xác nhận)
		Compression:  compress.Zstd,                  // Nén Zstandard giảm 60-70% dung lượng
		MaxAttempts:  5,                              // Retry 5 lần khi gặp sự cố tạm thời
		BatchTimeout: 10 * time.Millisecond,          // Flush nhịp micro-batch 10ms
		BatchSize:    100,                            // Batch tối đa 100 tin nhắn
		Async:        false,                          // Đồng bộ để nhận kết quả xác nhận ngay (Fail-Close)
	}

	// 2. Cấu hình Writer cho Invalid DLQ Topic (pubg.v1.invalid)
	invalidWriter := &kafka.Writer{
		Addr:         kafka.TCP(brokers...),
		Topic:        invalidTopic,
		Balancer:     &kafka.Hash{},
		RequiredAcks: kafka.RequireAll,
		Compression:  compress.Zstd,
		MaxAttempts:  5,
		BatchTimeout: 10 * time.Millisecond,
		BatchSize:    100,
		Async:        false,
	}

	log.WithFields(logrus.Fields{
		"brokers":       brokers,
		"raw_topic":     rawTopic,
		"invalid_topic": invalidTopic,
		"acks":          "all",
		"compression":   "zstd",
	}).Info("Đã khởi tạo thành công Go Kafka Producer (HA & Fail-Close ready)")

	return &KafkaProducer{
		rawWriter:     rawWriter,
		invalidWriter: invalidWriter,
		log:           log,
	}, nil
}

// ProduceEvent phát tin nhắn EventEnvelope hợp lệ vào Kafka (Key = match_id)
func (kp *KafkaProducer) ProduceEvent(ctx context.Context, envelope *contract.EventEnvelope) error {
	if envelope == nil {
		return fmt.Errorf("envelope không được phép nil")
	}

	// Serialize EventEnvelope thành byte JSON
	payloadBytes, err := json.Marshal(envelope)
	if err != nil {
		return fmt.Errorf("serialize EventEnvelope thất bại: %w", err)
	}

	// Tạo Kafka Message với Message Key = match_id để đảm bảo Strict Ordering per match
	msg := kafka.Message{
		Key:   []byte(envelope.MatchID),
		Value: payloadBytes,
		Time:  envelope.IngestTime,
	}

	// Gửi tin nhắn vào Raw Topic
	if err := kp.rawWriter.WriteMessages(ctx, msg); err != nil {
		kp.log.WithFields(logrus.Fields{
			"event_id": envelope.EventID,
			"match_id": envelope.MatchID,
		}).WithError(err).Error("Phát tin nhắn Kafka thất bại (Fail-Close Triggered)")
		return fmt.Errorf("phát tin nhắn Kafka thất bại vào topic %s: %w", kp.rawWriter.Topic, err)
	}

	return nil
}

// ProduceInvalid phát tin nhắn InvalidRecord bị vi phạm vào DLQ Kafka Topic (Key = source_file)
func (kp *KafkaProducer) ProduceInvalid(ctx context.Context, invalid *contract.InvalidRecord) error {
	if invalid == nil {
		return fmt.Errorf("invalid record không được phép nil")
	}

	payloadBytes, err := json.Marshal(invalid)
	if err != nil {
		return fmt.Errorf("serialize InvalidRecord thất bại: %w", err)
	}

	msg := kafka.Message{
		Key:   []byte(invalid.SourceFile),
		Value: payloadBytes,
		Time:  invalid.FailedAt,
	}

	if err := kp.invalidWriter.WriteMessages(ctx, msg); err != nil {
		kp.log.WithFields(logrus.Fields{
			"source_file":  invalid.SourceFile,
			"record_index": invalid.RecordIndex,
		}).WithError(err).Error("Phát tin nhắn Invalid DLQ Kafka thất bại (Fail-Close Triggered)")
		return fmt.Errorf("phát tin nhắn Invalid DLQ thất bại vào topic %s: %w", kp.invalidWriter.Topic, err)
	}

	return nil
}

// Close đóng các Kafka writers và flush dữ liệu tồn đọng an toàn
func (kp *KafkaProducer) Close() error {
	var errs []string

	if err := kp.rawWriter.Close(); err != nil {
		errs = append(errs, fmt.Sprintf("đóng rawWriter thất bại: %v", err))
	}
	if err := kp.invalidWriter.Close(); err != nil {
		errs = append(errs, fmt.Sprintf("đóng invalidWriter thất bại: %v", err))
	}

	if len(errs) > 0 {
		return fmt.Errorf("lỗi đóng Kafka Producer: %s", fmt.Sprint(errs))
	}
	kp.log.Info("Đã đóng kết nối Kafka Producer an toàn.")
	return nil
}
