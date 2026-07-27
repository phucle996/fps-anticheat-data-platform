mod config;
mod domain;
mod error;
mod ingest;
mod transform;

use config::Config;
use error::Result;
use ingest::{BatchAccumulator, BatchAccumulatorConfig, KafkaConsumer};
use std::time::Duration;
use tracing::{info, warn};
use tracing_subscriber::{layer::SubscriberExt, util::SubscriberInitExt, EnvFilter};
use transform::EventValidator;

#[tokio::main]
async fn main() -> Result<()> {
	// 1. Khởi tạo Structured JSON Logging chuẩn Cloud-Native
	let env_filter = EnvFilter::try_from_default_env().unwrap_or_else(|_| EnvFilter::new("info"));
	let json_formatting = tracing_subscriber::fmt::layer().json();

	tracing_subscriber::registry()
		.with(env_filter)
		.with(json_formatting)
		.init();

	info!("Khởi động tiến trình Rust Stream Processor Engine (Phase 14 Data Quality Active)...");

	// 2. Nạp cấu hình ứng dụng từ biến môi trường (Fail-Close 100%)
	let config = match Config::from_env() {
		Ok(cfg) => cfg,
		Err(err) => {
			warn!("Dừng chương trình do lỗi nạp cấu hình (Fail-Close Triggered): {}", err);
			return Err(err);
		}
	};

	info!(
		kafka_brokers = %config.kafka_brokers,
		raw_topic = %config.kafka_raw_topic,
		group_id = %config.kafka_group_id,
		minio_endpoint = %config.minio_endpoint,
		minio_bucket = %config.minio_bucket,
		batch_size = config.batch_size,
		flush_interval_ms = config.flush_interval_ms,
		"Đã khởi tạo thành công cấu hình Rust Processor"
	);

	// 3. Khởi tạo KafkaConsumer với enable.auto.commit = false từ ingest module
	let consumer = KafkaConsumer::new(&config)?;
	info!("KafkaConsumer đã sẵn sàng nhận luồng sự kiện từ Kafka Topic: {}", config.kafka_raw_topic);

	// 4. Khởi tạo BatchAccumulator gom tụ dữ liệu RAM
	let accum_config = BatchAccumulatorConfig {
		max_records: config.batch_size,
		max_bytes: 10 * 1024 * 1024,
		flush_interval: Duration::from_millis(config.flush_interval_ms),
	};
	let mut accumulator = BatchAccumulator::new(accum_config);

	info!("Bắt đầu vòng lặp Consumer Loop (Data Quality Validation Active)...");
	loop {
		tokio::select! {
			_ = tokio::signal::ctrl_c() => {
				info!("Nhận tín hiệu Graceful Shutdown từ OS (Ctrl+C), thực thi Flush bộ đệm dư thừa...");
				if let Some(final_batch) = accumulator.flush() {
					let outcome = EventValidator::validate_batch(final_batch.events);
					info!(
						batch_id = %final_batch.batch_id,
						valid_count = outcome.valid_count,
						invalid_count = outcome.invalid_count,
						"Đã Flush và Validate thành công Batch cuối cùng khi Shutdown"
					);
				}
				break;
			}
			msg_result = consumer.recv_message() => {
				match msg_result {
					Ok(Some(msg)) => {
						if let Some(completed_batch) = accumulator.push(msg) {
							// Thực thi 11 quy tắc Data Quality Validation
							let outcome = EventValidator::validate_batch(completed_batch.events);
							info!(
								batch_id = %completed_batch.batch_id,
								valid_count = outcome.valid_count,
								invalid_count = outcome.invalid_count,
								partitions = ?completed_batch.partition_offsets,
								"Đã Validate Data Quality thành công (Bản ghi hợp lệ sẵn sàng cho Arrow & Parquet)"
							);
						}
					}
					Ok(None) => {
						// Bỏ qua tin nhắn rỗng hoặc malformed JSON
					}
					Err(err) => {
						warn!(error = %err, "Lỗi khi nhận tin nhắn Kafka (Fail-Close Pending)");
					}
				}
			}
		}

		if accumulator.should_flush_timer() {
			if let Some(completed_batch) = accumulator.flush() {
				let outcome = EventValidator::validate_batch(completed_batch.events);
				info!(
					batch_id = %completed_batch.batch_id,
					valid_count = outcome.valid_count,
					invalid_count = outcome.invalid_count,
					partitions = ?completed_batch.partition_offsets,
					"Đã Trigger Flush và Validate Data Quality thành công theo Timer Interval"
				);
			}
		}
	}

	info!("Tiến trình Rust Stream Processor kết thúc an toàn.");
	Ok(())
}
