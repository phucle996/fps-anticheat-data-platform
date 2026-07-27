mod config;
mod domain;
mod error;

use config::Config;
use error::Result;
use tracing::{info, warn};
use tracing_subscriber::{layer::SubscriberExt, util::SubscriberInitExt, EnvFilter};

#[tokio::main]
async fn main() -> Result<()> {
	// 1. Khởi tạo Structured JSON Logging chuẩn Cloud-Native
	let env_filter = EnvFilter::try_from_default_env().unwrap_or_else(|_| EnvFilter::new("info"));
	let json_formatting = tracing_subscriber::fmt::layer().json();

	tracing_subscriber::registry()
		.with(env_filter)
		.with(json_formatting)
		.init();

	info!("Khởi động tiến trình Rust Stream Processor Engine (Phase 11 Foundation)...");

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
		"Đã khởi tạo thành công cấu hình Rust Processor Nền Móng"
	);

	info!("Rust Stream Processor sẵn sàng tiếp nhận luồng xử lý từ Kafka!");
	Ok(())
}
