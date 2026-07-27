use crate::config::Config;
use crate::error::Result;
use crate::ingest::{BatchAccumulator, BatchAccumulatorConfig, CompletedBatch, KafkaConsumer};
use crate::storage::{BatchManifest, MinioWriter, PartitionOffsetMetadata};
use crate::transform::{ArrowConverter, EventDeduplicator, EventValidator, ParquetSerializer};
use crate::worker::RDynamicWorkerPool;
use chrono::Utc;
use std::collections::HashMap;
use std::time::Duration;
use tracing::{info, warn};

/// StreamProcessorApp đóng gói toàn bộ quy trình Ingest -> Validate -> Dedup -> Arrow -> Parquet -> MinIO -> Offset Commit -> Dynamic R Pool
pub struct StreamProcessorApp {
    config: Config,                // Cấu hình ứng dụng
    consumer: KafkaConsumer,       // Bộ đọc tin nhắn Kafka
    accumulator: BatchAccumulator, // Bộ gom batch dữ liệu RAM
    writer: MinioWriter,           // Bộ ghi dữ liệu MinIO S3
    r_pool: RDynamicWorkerPool,    // Dynamic R Worker Pool Daemon
}

impl StreamProcessorApp {
    /// New khởi tạo StreamProcessorApp và khởi tạo kết nối hạ tầng
    pub fn new(config: Config) -> Result<Self> {
        let consumer = KafkaConsumer::new(&config)?;
        let writer = MinioWriter::new(&config)?;
        let r_pool = RDynamicWorkerPool::new(config.r_max_workers);

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
        })
    }

    /// Run khởi chạy vòng lặp async tokio lắng nghe sự kiện Kafka và tín hiệu Graceful Shutdown
    pub async fn run(&mut self) -> Result<()> {
        let _ = self.writer.ensure_datalake_structure().await;
        info!("Bắt đầu vòng lặp Consumer Loop trong StreamProcessorApp...");

        loop {
            tokio::select! {
                _ = tokio::signal::ctrl_c() => {
                    info!("Nhận tín hiệu Graceful Shutdown từ OS (Ctrl+C), thực thi Flush bộ đệm dư thừa...");
                    if let Some(final_batch) = self.accumulator.flush() {
                        let _ = self.process_completed_batch(final_batch).await;
                    }
                    break;
                }
                msg_result = self.consumer.recv_message() => {
                    match msg_result {
                        Ok(Some(msg)) => {
                            if let Some(completed_batch) = self.accumulator.push(msg) {
                                let _ = self.process_completed_batch(completed_batch).await;
                            }
                        }
                        Ok(None) => {}
                        Err(err) => {
                            warn!(error = %err, "Lỗi khi nhận tin nhắn Kafka (Fail-Close Pending)");
                        }
                    }
                }
            }

            // Kiểm tra Timer Flush theo nhịp flush_interval_ms
            if self.accumulator.should_flush_timer() {
                if let Some(completed_batch) = self.accumulator.flush() {
                    let _ = self.process_completed_batch(completed_batch).await;
                }
            }
        }

        info!("StreamProcessorApp kết thúc an toàn.");
        Ok(())
    }

    /// Process_completed_batch xử lý 1 CompletedBatch theo đúng quy trình Durable Two-Phase Commit & Dynamic R Worker Pool
    pub async fn process_completed_batch(&self, completed_batch: CompletedBatch) -> Result<()> {
        // 1. Data Quality Validation
        let val_outcome = EventValidator::validate_batch(completed_batch.events);

        // Upload bản ghi vi phạm (nếu có) sang bronze/invalid/
        if !val_outcome.invalid_records.is_empty() {
            let invalid_path = MinioWriter::generate_invalid_path(&completed_batch.batch_id, "now");
            if let Err(err) = self.writer.upload_invalid_records(&invalid_path, &val_outcome.invalid_records).await {
                warn!(error = %err, "Lỗi upload invalid records JSON sang MinIO");
            }
        }

        // 2. Deduplication trong Batch
        let dedup_outcome = EventDeduplicator::deduplicate_batch(val_outcome.valid_records);

        if !dedup_outcome.unique_records.is_empty() {
            // 3. Arrow RecordBatch Conversion
            let record_batch = ArrowConverter::events_to_record_batch(&dedup_outcome.unique_records)?;

            // 4. Parquet Zstandard Serialization
            let parquet_bytes = ParquetSerializer::record_batch_to_parquet_bytes(&record_batch)?;

            // 5. MinIO S3 Bronze Parquet Upload (Pha 1)
            let sample_time = dedup_outcome.unique_records[0].ingest_time.clone();
            let bronze_path = MinioWriter::generate_bronze_path(&completed_batch.batch_id, &sample_time);
            let checksum = self.writer.upload_parquet(&bronze_path, parquet_bytes).await?;

            // 6. MinIO S3 Manifest Audit Log Upload (Pha 2)
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
            let manifest_path = MinioWriter::generate_manifest_path(&completed_batch.batch_id, &sample_time);
            self.writer.upload_manifest(&manifest_path, &manifest).await?;

            // 7. Commit Kafka Offsets (CHỈ THỰC THI SAU KHI PHA 1 VÀ 2 THÀNH CÔNG)
            if let Err(err) = self.consumer.commit_partition_offsets(&completed_batch.partition_offsets) {
                warn!(error = %err, "Lỗi commit Kafka offsets cho batch");
            } else {
                info!(
                    batch_id = %completed_batch.batch_id,
                    valid_count = val_outcome.valid_count,
                    dedup_count = dedup_outcome.unique_records.len(),
                    checksum = %checksum,
                    bronze_path = %bronze_path,
                    manifest_path = %manifest_path,
                    "Hoàn tất quy trình Durable Two-Phase Commit cho Processing Batch!"
                );

                // 8. Kích hoạt Spark-Style Dynamic R Worker Pool (Non-blocking Task Dispatch)
                self.r_pool.dispatch_manifest(manifest_path);
            }
        }

        Ok(())
    }
}

/// Helper create_manifest đóng gói BatchManifest audit log
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
        processing_timestamp: Utc::now().to_rfc3339(),
    }
}
