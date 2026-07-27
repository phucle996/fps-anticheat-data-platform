use crate::error::{AppError, Result};
use std::env;

/// Config chứa toàn bộ tham số môi trường của Rust Processor (Áp dụng Fail-Close 100%, ZERO Fallback)
#[derive(Debug, Clone)]
pub struct Config {
    pub kafka_brokers: String,       // Danh sách Kafka Brokers (Bắt buộc KAFKA_BROKERS)
    pub kafka_raw_topic: String,     // Kafka Topic đọc dữ liệu thô (Bắt buộc KAFKA_RAW_TOPIC)
    pub kafka_group_id: String,      // Kafka Consumer Group ID (Bắt buộc KAFKA_GROUP_ID)
    pub minio_endpoint: String,      // Endpoint của MinIO S3 Server (Bắt buộc MINIO_ENDPOINT)
    pub minio_access_key: String,   // Access Key của MinIO S3 (Bắt buộc MINIO_ACCESS_KEY)
    pub minio_secret_key: String,   // Secret Key của MinIO S3 (Bắt buộc MINIO_SECRET_KEY)
    pub minio_bucket: String,        // Bucket chính lưu dữ liệu Parquet (Bắt buộc MINIO_BUCKET)
    pub batch_size: usize,           // Kích thước batch tối đa cho Rust Engine (Mặc định: 1000 bản ghi)
    pub flush_interval_ms: u64,      // Thời gian timer flush tối đa (Mặc định: 1000ms)
}

impl Config {
    /// LoadFromEnv nạp cấu hình từ biến môi trường với kiểm tra Fail-Close nghiêm ngặt
    pub fn from_env() -> Result<Self> {
        let mut missing_vars = Vec::new();

        // Đọc từng biến môi trường và ghi nhận nếu thiếu
        let kafka_brokers = match env::var("KAFKA_BROKERS") {
            Ok(v) if !v.trim().is_empty() => v,
            _ => {
                missing_vars.push("KAFKA_BROKERS");
                String::new()
            }
        };

        let kafka_raw_topic = match env::var("KAFKA_RAW_TOPIC") {
            Ok(v) if !v.trim().is_empty() => v,
            _ => {
                missing_vars.push("KAFKA_RAW_TOPIC");
                String::new()
            }
        };

        let kafka_group_id = match env::var("KAFKA_GROUP_ID") {
            Ok(v) if !v.trim().is_empty() => v,
            _ => {
                missing_vars.push("KAFKA_GROUP_ID");
                String::new()
            }
        };

        let minio_endpoint = match env::var("MINIO_ENDPOINT") {
            Ok(v) if !v.trim().is_empty() => v,
            _ => {
                missing_vars.push("MINIO_ENDPOINT");
                String::new()
            }
        };

        let minio_access_key = match env::var("MINIO_ACCESS_KEY") {
            Ok(v) if !v.trim().is_empty() => v,
            _ => {
                missing_vars.push("MINIO_ACCESS_KEY");
                String::new()
            }
        };

        let minio_secret_key = match env::var("MINIO_SECRET_KEY") {
            Ok(v) if !v.trim().is_empty() => v,
            _ => {
                missing_vars.push("MINIO_SECRET_KEY");
                String::new()
            }
        };

        let minio_bucket = match env::var("MINIO_BUCKET") {
            Ok(v) if !v.trim().is_empty() => v,
            _ => {
                missing_vars.push("MINIO_BUCKET");
                String::new()
            }
        };

        // Nếu thiếu bất kỳ biến nào -> Fail-Close ngắt tiến trình ngay lập tức
        if !missing_vars.is_empty() {
            return Err(AppError::Config(format!(
                "Phát hiện {} biến môi trường chưa khai báo: [{}] (Fail-Close Rule Violated)",
                missing_vars.len(),
                missing_vars.join(", ")
            )));
        }

        // Đọc các tham số tuning batch nếu được cung cấp
        let batch_size = env::var("RUST_BATCH_SIZE")
            .ok()
            .and_then(|v| v.parse().ok())
            .unwrap_or(1000);

        let flush_interval_ms = env::var("RUST_FLUSH_INTERVAL_MS")
            .ok()
            .and_then(|v| v.parse().ok())
            .unwrap_or(1000);

        Ok(Config {
            kafka_brokers,
            kafka_raw_topic,
            kafka_group_id,
            minio_endpoint,
            minio_access_key,
            minio_secret_key,
            minio_bucket,
            batch_size,
            flush_interval_ms,
        })
    }
}
