package kafka

import (
	"context"
	"encoding/json"
	"fmt"
	"time"

	"github.com/segmentio/kafka-go"
	"github.com/segmentio/kafka-go/compress"
	"github.com/sirupsen/logrus"

	"go-ingestor/internal/contract"
	"go-ingestor/internal/pipeline"
)

// KafkaProducer triển khai Producer interface của pipeline bằng thư viện Pure Go segmentio/kafka-go
// Cấu hình nghiêm ngặt cho môi trường Cloud-Native High Availability (HA) & Fail-Close
type KafkaProducer struct {
	rawWriter     *kafka.Writer // Writer phát dữ liệu hợp lệ vào Raw Topic (pubg.v1.player-stat.raw)
	invalidWriter *kafka.Writer // Writer phát dữ liệu lỗi vào DLQ Topic (pubg.v1.invalid)
	log           *logrus.Entry // Logger JSON Logrus
}

// NewKafkaProducer khởi tạo KafkaProducer với cấu hình HA & Fail-Close (acks=all, Gzip Compression)
func NewKafkaProducer(brokers []string, rawTopic, invalidTopic string, log *logrus.Entry) (*KafkaProducer, error) {
	// 1. Cấu hình Writer cho Raw Topic (pubg.v1.player-stat.raw)
	rawWriter := &kafka.Writer{
		Addr:         kafka.TCP(brokers...),
		Topic:        rawTopic,
		Balancer:     &kafka.Hash{},         // Partitioning theo Message Key (match_id) để đảm bảo Per-Match Strict Order
		RequiredAcks: kafka.RequireAll,      // acks=all (Bắt buộc toàn bộ ISR Replicas xác nhận trước khi ACK)
		Compression:  compress.Gzip,         // Nén Gzip tiết kiệm bandwidth mạng
		MaxAttempts:  5,                     // Retry 5 lần khi gặp sự cố mạng tạm thời
		BatchTimeout: 10 * time.Millisecond, // Flush nhịp micro-batch 10ms
		BatchSize:    100,                   // Gom batch tối đa 100 tin nhắn per network flush request
		Async:        false,                 // Gửi đồng bộ để nhận xác nhận ngay lập tức (Fail-Close Enforced)
	}

	// 2. Cấu hình Writer cho Invalid DLQ Topic (pubg.v1.invalid)
	invalidWriter := &kafka.Writer{
		Addr:         kafka.TCP(brokers...),
		Topic:        invalidTopic,
		Balancer:     &kafka.Hash{},
		RequiredAcks: kafka.RequireAll,
		Compression:  compress.Gzip,
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
		"compression":   "gzip",
	}).Info("Khởi tạo thành công Kafka Producer (HA & Fail-Close Ready)")

	return &KafkaProducer{
		rawWriter:     rawWriter,
		invalidWriter: invalidWriter,
		log:           log,
	}, nil
}

// ProduceEvent phát tin nhắn EventEnvelope hợp lệ vào Kafka Raw Topic (Key = match_id)
func (kp *KafkaProducer) ProduceEvent(ctx context.Context, envelope *contract.EventEnvelope) error {
	payloadBytes, err := json.Marshal(envelope)
	if err != nil {
		return fmt.Errorf("serialize EventEnvelope thất bại: %w", err)
	}

	msg := kafka.Message{
		Key:   []byte(envelope.MatchID),
		Value: payloadBytes,
		Time:  envelope.IngestTime,
	}

	if err := kp.rawWriter.WriteMessages(ctx, msg); err != nil {
		kp.log.WithFields(logrus.Fields{
			"event_id": envelope.EventID,
			"match_id": envelope.MatchID,
		}).WithError(err).Error("Phát tin nhắn Kafka thất bại (Fail-Close Triggered)")
		return fmt.Errorf("phát tin nhắn Kafka thất bại vào topic %s: %w", kp.rawWriter.Topic, err)
	}

	return nil
}

// ProduceKillEvent phát tin nhắn KillEventEnvelope hợp lệ vào Kafka Raw Topic (Key = match_id)
func (kp *KafkaProducer) ProduceKillEvent(ctx context.Context, envelope *contract.KillEventEnvelope) error {
	payloadBytes, err := json.Marshal(envelope)
	if err != nil {
		return fmt.Errorf("serialize KillEventEnvelope thất bại: %w", err)
	}

	msg := kafka.Message{
		Key:   []byte(envelope.MatchID),
		Value: payloadBytes,
		Time:  envelope.IngestTime,
	}

	if err := kp.rawWriter.WriteMessages(ctx, msg); err != nil {
		kp.log.WithFields(logrus.Fields{
			"event_id": envelope.EventID,
			"match_id": envelope.MatchID,
		}).WithError(err).Error("Phát tin nhắn KillEvent Kafka thất bại (Fail-Close Triggered)")
		return fmt.Errorf("phát tin nhắn KillEvent Kafka thất bại vào topic %s: %w", kp.rawWriter.Topic, err)
	}

	return nil
}

// ProduceInvalid phát tin nhắn InvalidRecord bị vi phạm vào DLQ Kafka Topic (Key = source_file)
func (kp *KafkaProducer) ProduceInvalid(ctx context.Context, invalid *contract.InvalidRecord) error {
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

// Compile-time interface assertion
var _ pipeline.Producer = (*KafkaProducer)(nil)
