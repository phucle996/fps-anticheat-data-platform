use crate::config::Config;
use crate::domain::EventEnvelope;
use crate::error::{AppError, Result};
use rdkafka::config::ClientConfig;
use rdkafka::consumer::{Consumer, StreamConsumer};
use rdkafka::message::Message;
use rdkafka::TopicPartitionList;
use std::collections::HashMap;
use std::sync::Arc;
use tracing::{error, info, warn};

/// ConsumedMessage đại diện cho 1 sự kiện đã được giải mã kèm metadata vị trí Kafka
#[derive(Debug, Clone)]
pub struct ConsumedMessage {
    pub envelope: EventEnvelope, // Structural Payload được chuẩn hóa
    pub topic: String,           // Topic Kafka nguồn
    pub partition: i32,          // Partition ID
    pub offset: i64,             // Offset ID
    pub key: Option<String>,     // Message Key (match_id)
}

/// KafkaConsumer bao bọc StreamConsumer từ librdkafka với cấu hình At-Least-Once
pub struct KafkaConsumer {
    consumer: Arc<StreamConsumer>, // StreamConsumer thread-safe của rdkafka
    topic: String,                 // Topic đang subscribe (pubg.v1.player-stat.raw)
}

impl KafkaConsumer {
    /// New khởi tạo KafkaConsumer từ Config (tắt auto commit, auto.offset.reset = earliest)
    pub fn new(config: &Config) -> Result<Self> {
        // Cấu hình rdkafka ClientConfig chuẩn Cloud-Native High-Availability
        let consumer: StreamConsumer = ClientConfig::new()
            .set("bootstrap.servers", &config.kafka_brokers)
            .set("group.id", &config.kafka_group_id)
            .set("enable.auto.commit", "false")        // Tắt commit tự động (At-Least-Once Delivery)
            .set("enable.auto.offset.store", "false") // Quản lý lưu offset thủ công
            .set("auto.offset.reset", "earliest")     // Đọc từ đầu nếu là consumer group mới
            .set("session.timeout.ms", "45000")       // Heartbeat timeout 45 giây
            .create()
            .map_err(|e| AppError::Kafka(format!("Tạo Kafka StreamConsumer thất bại: {}", e)))?;

        // Subscribe vào Topic Kafka
        consumer
            .subscribe(&[&config.kafka_raw_topic])
            .map_err(|e| AppError::Kafka(format!("Subscribe topic {} thất bại: {}", config.kafka_raw_topic, e)))?;

        info!(
            brokers = %config.kafka_brokers,
            topic = %config.kafka_raw_topic,
            group_id = %config.kafka_group_id,
            "Đã khởi tạo KafkaConsumer thuộc module ingest (At-Least-Once Active)"
        );

        Ok(Self {
            consumer: Arc::new(consumer),
            topic: config.kafka_raw_topic.clone(),
        })
    }

    /// Verify_topic_exists kiểm tra topic tồn tại trên Kafka broker tại startup (TC-04 Fix)
    /// Mục đích: Phát hiện sai tên topic TRƯỚC khi vào consume loop vô hạn
    /// Cơ chế: fetch_metadata với timeout ngắn (10s) — nếu topic không có partition → Fail-Close ngay
    pub async fn verify_topic_exists(&self) -> Result<()> {
        use rdkafka::consumer::Consumer;
        use std::time::Duration;

        // fetch_metadata gọi Kafka broker để lấy thông tin partition của topic cụ thể
        // Timeout 10 giây để tránh treo vô hạn khi broker không reachable
        let metadata = self.consumer
            .fetch_metadata(Some(&self.topic), Duration::from_secs(10))
            .map_err(|e| AppError::Kafka(format!(
                "Không thể fetch metadata topic '{}' từ Kafka broker: {} (Fail-Close Triggered)",
                self.topic, e
            )))?;

        // Kiểm tra topic có tồn tại không (có ít nhất 1 partition)
        let topic_meta = metadata
            .topics()
            .iter()
            .find(|t| t.name() == self.topic);

        match topic_meta {
            None => {
                // Topic không xuất hiện trong metadata → không tồn tại
                Err(AppError::Kafka(format!(
                    "Topic '{}' không tồn tại trên Kafka broker (Fail-Close Triggered)",
                    self.topic
                )))
            }
            Some(topic) if topic.partitions().is_empty() => {
                // Topic tồn tại nhưng không có partition → lỗi cấu hình broker
                Err(AppError::Kafka(format!(
                    "Topic '{}' tồn tại nhưng không có partition nào (Fail-Close Triggered)",
                    self.topic
                )))
            }
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


    /// Recv_message nhận tin nhắn tiếp theo từ Kafka Stream, giải mã JSON an toàn (Resilient Loop)
    pub async fn recv_message(&self) -> Result<Option<ConsumedMessage>> {
        match self.consumer.recv().await {
            Ok(borrowed_msg) => {
                let topic = borrowed_msg.topic().to_string();
                let partition = borrowed_msg.partition();
                let offset = borrowed_msg.offset();

                // Lấy Message Key (match_id)
                let key = borrowed_msg
                    .key()
                    .and_then(|k| std::str::from_utf8(k).ok())
                    .map(|s| s.to_string());

                // Lấy Payload byte array
                let payload_bytes = match borrowed_msg.payload() {
                    Some(bytes) => bytes,
                    None => {
                        warn!(
                            topic = %topic,
                            partition = partition,
                            offset = offset,
                            "Bỏ qua tin nhắn Kafka rỗng (Empty Payload)"
                        );
                        return Ok(None);
                    }
                };

                // Giải mã JSON sang EventEnvelope (Resilient Handling đối với Malformed JSON)
                match serde_json::from_slice::<EventEnvelope>(payload_bytes) {
                    Ok(envelope) => Ok(Some(ConsumedMessage {
                        envelope,
                        topic,
                        partition,
                        offset,
                        key,
                    })),
                    Err(err) => {
                        // Malformed JSON: Log cảnh báo và bỏ qua mà không làm sập Consumer Loop
                        error!(
                            topic = %topic,
                            partition = partition,
                            offset = offset,
                            error = %err,
                            "Phát hiện Malformed JSON Payload vi phạm hợp đồng, bỏ qua bản ghi lỗi"
                        );
                        Ok(None)
                    }
                }
            }
            Err(err) => Err(AppError::Kafka(format!("Lỗi poll Kafka message: {}", err))),
        }
    }

    /// Commit_offset thực thi commit thủ công offset cho 1 partition đơn lẻ
    pub fn commit_offset(&self, partition: i32, offset: i64) -> Result<()> {
        let mut tpl = TopicPartitionList::new();
        // Commit offset + 1 theo chuẩn Kafka specification
        tpl.add_partition_offset(&self.topic, partition, rdkafka::Offset::Offset(offset + 1))
            .map_err(|e| AppError::Kafka(format!("Thêm TopicPartitionList offset thất bại: {}", e)))?;

        self.consumer
            .commit(&tpl, rdkafka::consumer::CommitMode::Sync)
            .map_err(|e| AppError::Kafka(format!("Commit offset {}/{} thất bại: {}", partition, offset, e)))?;

        info!(
            topic = %self.topic,
            partition = partition,
            committed_offset = offset + 1,
            "Đã commit offset thủ công thành công sang Kafka Cluster"
        );

        Ok(())
    }

    /// Commit_partition_offsets commit offset cho tất cả các partition trong batch cùng lúc
    pub fn commit_partition_offsets(&self, partition_offsets: &HashMap<i32, (i64, i64)>) -> Result<()> {
        if partition_offsets.is_empty() {
            return Ok(());
        }

        let mut tpl = TopicPartitionList::new();
        for (partition, (_min_off, max_off)) in partition_offsets {
            // Commit max_offset + 1 cho từng partition
            tpl.add_partition_offset(&self.topic, *partition, rdkafka::Offset::Offset(*max_off + 1))
                .map_err(|e| AppError::Kafka(format!("Thêm Partition {} offset {} thất bại: {}", partition, max_off + 1, e)))?;
        }

        self.consumer
            .commit(&tpl, rdkafka::consumer::CommitMode::Sync)
            .map_err(|e| AppError::Kafka(format!("Commit multi-partition offsets thất bại: {}", e)))?;

        info!(
            topic = %self.topic,
            partitions_count = partition_offsets.len(),
            "Đã commit thành công Kafka offsets cho toàn bộ Partitions trong Batch (Two-Phase Commit Completed)"
        );

        Ok(())
    }
}
