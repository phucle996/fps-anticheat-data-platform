use crate::config::Config;
use crate::domain::AnyEnvelope;
use crate::error::{AppError, Result};
use rdkafka::config::ClientConfig;
use rdkafka::consumer::{Consumer, StreamConsumer};
use rdkafka::message::Message;
use rdkafka::TopicPartitionList;
use std::collections::HashMap;
use std::sync::Arc;
use tracing::{error, info, warn};

/// ConsumeOutcome định nghĩa kết quả phân loại khi consume 1 Kafka message
#[derive(Debug, Clone)]
pub enum ConsumeOutcome {
    Valid(ConsumedMessage),
    Invalid(InvalidKafkaMessage),
}

/// ConsumedMessage đại diện cho 1 sự kiện hợp lệ
#[derive(Debug, Clone)]
pub struct ConsumedMessage {
    pub envelope: AnyEnvelope,   // AnyEnvelope (KillEventEnvelope hoặc EventEnvelope)
    pub topic: String,
    pub partition: i32,
    pub offset: i64,
    pub key: Option<String>,
}

/// InvalidKafkaMessage đại diện cho 1 message bị malformed/empty để đẩy DLQ
#[derive(Debug, Clone)]
pub struct InvalidKafkaMessage {
    pub topic: String,
    pub partition: i32,
    pub offset: i64,
    pub raw_payload: Vec<u8>,
    pub error_reason: String,
}

/// KafkaConsumer bao bọc StreamConsumer từ librdkafka với cấu hình At-Least-Once
pub struct KafkaConsumer {
    consumer: Arc<StreamConsumer>, // StreamConsumer thread-safe của rdkafka
    topic: String,                 // Topic đang subscribe
}

impl KafkaConsumer {
    /// New khởi tạo KafkaConsumer từ Config
    pub fn new(config: &Config) -> Result<Self> {
        let consumer: StreamConsumer = ClientConfig::new()
            .set("bootstrap.servers", &config.kafka_brokers)
            .set("group.id", &config.kafka_group_id)
            .set("enable.auto.commit", "false")        // Tắt commit tự động (At-Least-Once Delivery)
            .set("enable.auto.offset.store", "false") // Quản lý lưu offset thủ công
            .set("auto.offset.reset", "earliest")     // Đọc từ đầu nếu là consumer group mới
            .set("session.timeout.ms", "45000")       // Heartbeat timeout 45 giây
            .create()
            .map_err(|e| AppError::Kafka(format!("Tạo Kafka StreamConsumer thất bại: {}", e)))?;

        consumer
            .subscribe(&[&config.kafka_raw_topic])
            .map_err(|e| AppError::Kafka(format!("Subscribe topic {} thất bại: {}", config.kafka_raw_topic, e)))?;

        info!(
            brokers = %config.kafka_brokers,
            topic = %config.kafka_raw_topic,
            group_id = %config.kafka_group_id,
            "Đã khởi tạo KafkaConsumer (At-Least-Once Active)"
        );

        Ok(Self {
            consumer: Arc::new(consumer),
            topic: config.kafka_raw_topic.clone(),
        })
    }

    /// Verify_topic_exists kiểm tra topic tồn tại trên Kafka broker tại startup
    pub async fn verify_topic_exists(&self) -> Result<()> {
        use std::time::Duration;

        let metadata = self.consumer
            .fetch_metadata(Some(&self.topic), Duration::from_secs(10))
            .map_err(|e| AppError::Kafka(format!(
                "Không thể fetch metadata topic '{}' từ Kafka broker: {}",
                self.topic, e
            )))?;

        let topic_meta = metadata
            .topics()
            .iter()
            .find(|t| t.name() == self.topic);

        match topic_meta {
            None => Err(AppError::Kafka(format!(
                "Topic '{}' không tồn tại trên Kafka broker (Fail-Close Triggered)",
                self.topic
            ))),
            Some(topic) if topic.partitions().is_empty() => Err(AppError::Kafka(format!(
                "Topic '{}' không có partition nào (Fail-Close Triggered)",
                self.topic
            ))),
            Some(topic) => {
                info!(
                    topic = %self.topic,
                    partition_count = topic.partitions().len(),
                    "Topic Kafka đã xác minh tồn tại và có đủ partition"
                );
                Ok(())
            }
        }
    }

    /// Recv_outcome nhận tin nhắn tiếp theo từ Kafka Stream và phân loại thành Valid hoặc Invalid DLQ
    pub async fn recv_outcome(&self) -> Result<ConsumeOutcome> {
        match self.consumer.recv().await {
            Ok(borrowed_msg) => {
                let topic = borrowed_msg.topic().to_string();
                let partition = borrowed_msg.partition();
                let offset = borrowed_msg.offset();

                let key = borrowed_msg
                    .key()
                    .and_then(|k| std::str::from_utf8(k).ok())
                    .map(|s| s.to_string());

                let payload_bytes = match borrowed_msg.payload() {
                    Some(bytes) => bytes.to_vec(),
                    None => {
                        warn!(
                            topic = %topic, partition = partition, offset = offset,
                            "Chuyển tin nhắn rỗng (Empty Payload) sang DLQ"
                        );
                        return Ok(ConsumeOutcome::Invalid(InvalidKafkaMessage {
                            topic,
                            partition,
                            offset,
                            raw_payload: vec![],
                            error_reason: "Empty payload".to_string(),
                        }));
                    }
                };

                // Thử parse JSON sang AnyEnvelope (chấp nhận cả KillEventEnvelope lẫn EventEnvelope)
                match serde_json::from_slice::<AnyEnvelope>(&payload_bytes) {
                    Ok(envelope) => Ok(ConsumeOutcome::Valid(ConsumedMessage {
                        envelope,
                        topic,
                        partition,
                        offset,
                        key,
                    })),
                    Err(err) => {
                        error!(
                            topic = %topic, partition = partition, offset = offset, error = %err,
                            "Phát hiện Malformed JSON Payload, đưa bản ghi hỏng vào DLQ"
                        );
                        Ok(ConsumeOutcome::Invalid(InvalidKafkaMessage {
                            topic,
                            partition,
                            offset,
                            raw_payload: payload_bytes,
                            error_reason: format!("Malformed JSON: {}", err),
                        }))
                    }
                }
            }
            Err(err) => Err(AppError::Kafka(format!("Lỗi poll Kafka message: {}", err))),
        }
    }

    /// Commit_partition_offsets commit offset cho tất cả các partition trong batch cùng lúc
    pub fn commit_partition_offsets(&self, partition_offsets: &HashMap<i32, (i64, i64)>) -> Result<()> {
        if partition_offsets.is_empty() {
            return Ok(());
        }

        let mut tpl = TopicPartitionList::new();
        for (partition, (_min_off, max_off)) in partition_offsets {
            tpl.add_partition_offset(&self.topic, *partition, rdkafka::Offset::Offset(*max_off + 1))
                .map_err(|e| AppError::Kafka(format!("Thêm Partition {} offset {} thất bại: {}", partition, max_off + 1, e)))?;
        }

        self.consumer
            .commit(&tpl, rdkafka::consumer::CommitMode::Sync)
            .map_err(|e| AppError::Kafka(format!("Commit multi-partition offsets thất bại: {}", e)))?;

        info!(
            topic = %self.topic,
            partitions_count = partition_offsets.len(),
            "Đã commit thành công Kafka offsets cho toàn bộ Partitions trong Batch"
        );

        Ok(())
    }
}
