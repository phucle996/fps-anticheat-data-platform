use crate::config::Config;
use crate::error::{AppError, Result};
use crate::ingest::{
    BatchAccumulator, BatchAccumulatorConfig, CompletedBatch, ConsumeOutcome, KafkaConsumer,
};
use crate::storage::{BatchManifest, MinioWriter, PartitionOffsetMetadata};
use crate::transform::{ArrowConverter, EventDeduplicator, EventValidator, ParquetSerializer};
use crate::transport::KafkaEventProducer;
use crate::worker::RDynamicWorkerPool;
use std::collections::HashMap;
use std::sync::Arc;
use std::time::Duration;
use tokio::time::MissedTickBehavior;
use tracing::{error, info, warn};

/// StreamProcessorApp đóng gói toàn bộ quy trình Ingest -> Validate -> Dedup -> Arrow -> Parquet -> MinIO -> Offset Commit -> Dynamic R Pool
pub struct StreamProcessorApp {
    config: Config,                // Cấu hình ứng dụng
    consumer: KafkaConsumer,       // Bộ đọc tin nhắn Kafka
    accumulator: BatchAccumulator, // Bộ gom batch dữ liệu RAM
    writer: Arc<MinioWriter>,      // Bộ ghi dữ liệu MinIO S3
    r_pool: RDynamicWorkerPool,    // Dynamic R Worker Pool Daemon
    event_producer: KafkaEventProducer,
}

impl StreamProcessorApp {
    /// New khởi tạo StreamProcessorApp và khởi tạo kết nối hạ tầng
    pub fn new(config: Config) -> Result<Self> {
        let consumer = KafkaConsumer::new(&config)?;
        let writer = MinioWriter::new(&config)?;
        let writer = Arc::new(writer);
        let r_pool = RDynamicWorkerPool::new(
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
            max_r_workers = config.r_max_workers,
            "Khởi tạo StreamProcessorApp engine (Dynamic R Pool Active) thành công"
        );

        Ok(Self {
            config,
            consumer,
            accumulator,
            writer,
            r_pool,
            event_producer,
        })
    }

    /// Run khởi chạy vòng lặp async tokio lắng nghe sự kiện Kafka và tín hiệu Graceful Shutdown
    pub async fn run(&mut self) -> Result<()> {
        self.writer.preflight_check().await?;
        self.writer.ensure_datalake_structure().await?;
        self.consumer.verify_topic_exists().await?;

        info!("Bắt đầu vòng lặp StreamProcessorApp consume loop (At-Least-Once Active)...");

        let mut flush_ticker =
            tokio::time::interval(Duration::from_millis(self.config.flush_interval_ms));
        flush_ticker.set_missed_tick_behavior(MissedTickBehavior::Delay);

        loop {
            tokio::select! {
                _ = tokio::signal::ctrl_c() => {
                    info!("Nhận tín hiệu SIGINT/SIGTERM, bắt đầu Graceful Shutdown...");
                    if let Some(final_batch) = self.accumulator.flush() {
                        info!("Flushing final batch trước khi ngắt process...");
                        if let Err(err) = self.process_completed_batch(final_batch).await {
                            error!(error = %err, "Lỗi flush final batch khi shutdown");
                            return Err(err);
                        }
                    }
                    break;
                }

                outcome = self.consumer.recv_outcome() => {
                    match outcome {
                        Ok(ConsumeOutcome::Valid(msg)) => {
                            if let Some(completed_batch) = self.accumulator.push(msg) {
                                if let Err(err) = self.process_completed_batch(completed_batch).await {
                                    error!(error = %err, "Batch processing thất bại — ngắt loop để container restart");
                                    return Err(err);
                                }
                            }
                        }
                        Ok(ConsumeOutcome::Invalid(invalid_msg)) => {
                            let partition = invalid_msg.partition;
                            let offset = invalid_msg.offset;
                            let reason = invalid_msg.error_reason.clone();
                            if let Some(completed_batch) = self.accumulator.push_invalid(invalid_msg) {
                                if let Err(err) = self.process_completed_batch(completed_batch).await {
                                    error!(error = %err, "Malformed batch DLQ thất bại — ngắt loop để redeliver");
                                    return Err(err);
                                }
                            }
                            warn!(
                                partition = partition,
                                offset = offset,
                                reason = %reason,
                                "Bản ghi Kafka hỏng đã được giữ trong bounded DLQ batch"
                            );
                        }
                        Err(err) => {
                            error!(error = %err, "Lỗi poll Kafka message — ngắt consumer loop");
                            return Err(err);
                        }
                    }
                }

                _ = flush_ticker.tick() => {
                    // Timer phải nằm trong select: khi Kafka im lặng, recv().await không wake loop
                    // nên partial batch cuối sẽ không được commit nếu chỉ check timer sau select.
                    if self.accumulator.should_flush_timer() {
                        if let Some(completed_batch) = self.accumulator.flush() {
                            if let Err(err) = self.process_completed_batch(completed_batch).await {
                                error!(error = %err, "Timer-triggered batch processing thất bại — ngắt loop");
                                return Err(err);
                            }
                        }
                    }
                }
            }
        }

        info!("StreamProcessorApp kết thúc an toàn.");
        Ok(())
    }

    /// Durability order: DLQ/Bronze → manifest → Silver/Gold → gold.ready → offset commit.
    pub async fn process_completed_batch(&self, completed_batch: CompletedBatch) -> Result<()> {
        let malformed_count = completed_batch.malformed_messages.len();
        if malformed_count > 0 {
            let path = MinioWriter::generate_malformed_path(&completed_batch.batch_id, "now");
            self.writer
                .upload_malformed_messages(&path, &completed_batch.malformed_messages)
                .await?;
        }

        let val_outcome = EventValidator::validate_batch(completed_batch.events);

        if !val_outcome.invalid_records.is_empty() {
            let invalid_path = MinioWriter::generate_invalid_path(&completed_batch.batch_id, "now");
            self.writer
                .upload_invalid_records(&invalid_path, &val_outcome.invalid_records)
                .await
                .map_err(|e| {
                    AppError::Storage(format!(
                        "Fail-Close: Upload invalid records DLQ thất bại: {}",
                        e
                    ))
                })?;
        }

        let dedup_outcome = EventDeduplicator::deduplicate_batch(val_outcome.valid_records);

        if !dedup_outcome.unique_records.is_empty() {
            let record_batch =
                ArrowConverter::events_to_record_batch(&dedup_outcome.unique_records)?;
            let parquet_bytes = ParquetSerializer::record_batch_to_parquet_bytes(&record_batch)?;

            let sample_time = "now";
            let bronze_path = if self.config.kafka_raw_topic.contains("kill-event") {
                MinioWriter::generate_kill_events_path(&completed_batch.batch_id, sample_time)
            } else {
                MinioWriter::generate_bronze_path(&completed_batch.batch_id, sample_time)
            };

            let checksum = self
                .writer
                .upload_parquet(&bronze_path, parquet_bytes)
                .await?;

            let manifest = create_manifest(
                &self.config.kafka_raw_topic,
                &completed_batch.batch_id,
                &completed_batch.partition_offsets,
                completed_batch.record_count,
                val_outcome.valid_count,
                val_outcome.invalid_count + malformed_count,
                dedup_outcome.duplicate_count,
                &bronze_path,
                &checksum,
            );
            let manifest_path =
                MinioWriter::generate_manifest_path(&completed_batch.batch_id, sample_time);
            self.writer
                .upload_manifest(&manifest_path, &manifest)
                .await?;

            let r_result = self.r_pool.process_manifest(&manifest_path).await?;
            let gold_events = self
                .event_producer
                .publish_gold_ready(&completed_batch.batch_id, &r_result)
                .await?;

            // Offset chỉ được advance sau mọi durable side effect/projection của batch.
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
            || malformed_count > 0
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
                malformed_count = malformed_count,
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
