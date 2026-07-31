use crate::config::Config;
use crate::error::{AppError, Result};
use crate::ingest::{
    BatchAccumulator, BatchAccumulatorConfig, CompletedBatch, ConsumeOutcome, KafkaConsumer,
};
use crate::storage::{BatchManifest, MinioWriter, PartitionOffsetMetadata};
use crate::transform::{ArrowConverter, EventDeduplicator, EventValidator, ParquetSerializer};
use crate::transport::KafkaEventProducer;
use crate::worker::DynamicWorkerPool;
use std::collections::HashMap;
use std::sync::Arc;
use std::time::Duration;
use tokio::time::MissedTickBehavior;
use tracing::{error, info};

/// StreamProcessorApp đóng gói toàn bộ quy trình Ingest -> Validate -> Dedup -> Arrow -> Parquet -> MinIO -> Offset Commit -> Native Worker Pool
pub struct StreamProcessorApp {
    config: Config,                // Cấu hình ứng dụng
    consumer: KafkaConsumer,       // Bộ đọc tin nhắn Kafka
    accumulator: BatchAccumulator, // Bộ gom batch dữ liệu RAM
    writer: Arc<MinioWriter>,      // Bộ ghi dữ liệu MinIO S3
    worker_pool: DynamicWorkerPool,// Dynamic Native Worker Pool Daemon
    event_producer: KafkaEventProducer,
}

impl StreamProcessorApp {
    /// New khởi tạo StreamProcessorApp và khởi tạo kết nối hạ tầng
    pub fn new(config: Config) -> Result<Self> {
        let consumer = KafkaConsumer::new(&config)?;
        let writer = MinioWriter::new(&config)?;
        let writer = Arc::new(writer);
        let worker_pool = DynamicWorkerPool::new(
            config.r_max_workers,
            writer.clone(),
            Duration::from_secs(config.r_worker_timeout_seconds),
        );
        let event_producer = KafkaEventProducer::new(&config)?;

        let accum_config = BatchAccumulatorConfig {
            max_records: config.batch_size,
            max_bytes: 10 * 1024 * 1024,
            flush_interval: Duration::from_millis(config.flush_interval_ms),
        };
        let accumulator = BatchAccumulator::new(accum_config);

        info!(
            max_workers = config.r_max_workers,
            "Khởi tạo StreamProcessorApp engine (Native Rust Worker Pool Active) thành công"
        );

        Ok(Self {
            config,
            consumer,
            accumulator,
            writer,
            worker_pool,
            event_producer,
        })
    }

    /// Run khởi chạy event loop đọc tin nhắn theo thời gian thực
    pub async fn run(&mut self) -> Result<()> {
        let mut interval = tokio::time::interval(Duration::from_millis(100));
        interval.set_missed_tick_behavior(MissedTickBehavior::Skip);

        info!("StreamProcessorApp loop đã sẵn sàng tiếp nhận telemetry events...");

        loop {
            interval.tick().await;

            let outcome = match self.consumer.recv_outcome().await {
                Ok(out) => out,
                Err(err) => {
                    error!(error = %err, "Lỗi đọc tin nhắn từ Kafka consumer");
                    continue;
                }
            };

            let maybe_completed = match outcome {
                ConsumeOutcome::Valid(valid_msg) => self.accumulator.push(valid_msg),
                ConsumeOutcome::Invalid(invalid_msg) => self.accumulator.push_invalid(invalid_msg),
            };

            if let Some(completed_batch) = maybe_completed {
                self.process_completed_batch(completed_batch).await?;
            }
        }
    }

    async fn process_completed_batch(&mut self, completed_batch: CompletedBatch) -> Result<()> {
        info!(
            batch_id = %completed_batch.batch_id,
            record_count = completed_batch.record_count,
            "Bắt đầu xử lý batch telemetry"
        );

        let val_outcome = EventValidator::validate_batch(completed_batch.events);
        let dedup_outcome = EventDeduplicator::deduplicate_batch(val_outcome.valid_records);

        if !dedup_outcome.unique_records.is_empty() {
            let record_batch = ArrowConverter::events_to_record_batch(&dedup_outcome.unique_records)?;
            let parquet_bytes = ParquetSerializer::record_batch_to_parquet_bytes(&record_batch)?;

            let now_str = chrono::Utc::now().to_rfc3339();
            let bronze_path = MinioWriter::generate_bronze_path(&completed_batch.batch_id, &now_str);
            let checksum = self.writer.upload_parquet(&bronze_path, parquet_bytes).await?;

            let manifest = create_manifest(
                &self.config.kafka_raw_topic,
                &completed_batch.batch_id,
                &completed_batch.partition_offsets,
                completed_batch.record_count,
                val_outcome.valid_count,
                val_outcome.invalid_count,
                dedup_outcome.duplicate_count,
                &bronze_path,
                &checksum,
            );

            let manifest_path = MinioWriter::generate_manifest_path(&completed_batch.batch_id, &now_str);
            self.writer.upload_manifest(&manifest_path, &manifest).await?;

            let worker_result = self.worker_pool.process_manifest(&manifest_path).await?;
            let gold_events = self
                .event_producer
                .publish_gold_ready(&completed_batch.batch_id, &worker_result)
                .await?;

            self.consumer
                .commit_partition_offsets(&completed_batch.partition_offsets)
                .map_err(|e| {
                    AppError::Kafka(format!(
                        "Fail-Close: Commit partition offsets thất bại: {}",
                        e
                    ))
                })?;
            info!(
                batch_id = %completed_batch.batch_id,
                valid_count = val_outcome.valid_count,
                dedup_count = dedup_outcome.unique_records.len(),
                checksum = %checksum,
                bronze_path = %bronze_path,
                manifest_path = %manifest_path,
                gold_events = gold_events,
                "Hoàn tất ordered durable processing cho batch"
            );
        } else if val_outcome.invalid_count > 0
            || dedup_outcome.duplicate_count > 0
            || !completed_batch.malformed_messages.is_empty()
        {
            self.consumer
                .commit_partition_offsets(&completed_batch.partition_offsets)
                .map_err(|e| {
                    AppError::Kafka(format!(
                        "Fail-Close: Commit invalid-only partition offsets thất bại: {}",
                        e
                    ))
                })?;
            info!(
                batch_id = %completed_batch.batch_id,
                invalid_count = val_outcome.invalid_count,
                malformed_count = completed_batch.malformed_messages.len(),
                duplicate_count = dedup_outcome.duplicate_count,
                "Đã commit Kafka offsets cho batch 100% invalid/duplicate"
            );
        }

        Ok(())
    }
}

fn create_manifest(
    topic: &str,
    batch_id: &str,
    offsets: &HashMap<i32, (i64, i64)>,
    total: usize,
    valid: usize,
    invalid: usize,
    duplicate: usize,
    data_path: &str,
    checksum: &str,
) -> BatchManifest {
    let mut partition_offsets = HashMap::new();
    for (part, (min, max)) in offsets {
        partition_offsets.insert(
            *part,
            PartitionOffsetMetadata {
                min_offset: *min,
                max_offset: *max,
            },
        );
    }

    BatchManifest {
        batch_id: batch_id.to_string(),
        source_topic: topic.to_string(),
        partition_offsets,
        total_records_read: total,
        valid_records_count: valid,
        invalid_records_count: invalid,
        duplicate_records_count: duplicate,
        data_object_path: data_path.to_string(),
        checksum_sha256: checksum.to_string(),
        processing_timestamp: chrono::Utc::now().to_rfc3339(),
    }
}
