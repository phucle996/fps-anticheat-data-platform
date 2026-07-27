use crate::error::{AppError, Result};
use std::env;

/// Config lưu trữ toàn bộ cấu hình ứng dụng được nạp từ biến môi trường
#[derive(Debug, Clone)]
#[allow(dead_code)]
pub struct Config {
    pub kafka_brokers: String,      // Danh sách Kafka brokers (vd: "localhost:9092")
    pub kafka_raw_topic: String,    // Raw Kafka topic (vd: "pubg.v1.player-stat.raw")
    pub kafka_group_id: String,     // Consumer group ID (vd: "rust-processor-group")
    pub minio_endpoint: String,     // Endpoint MinIO S3 (vd: "http://localhost:9000")
    pub minio_bucket: String,       // Bucket Data Lake MinIO (vd: "fps-anticheat-datalake")
    pub minio_access_key: String,   // Access Key của MinIO S3
    pub minio_secret_key: String,   // Secret Key của MinIO S3
    pub batch_size: usize,          // Ngưỡng số lượng bản ghi cho mỗi batch (vd: 1000)
    pub flush_interval_ms: u64,     // Ngưỡng thời gian flush batch theo ms (vd: 1000)
    pub r_max_workers: usize,       // Số lượng R Worker song song tối đa (phát hiện từ CPU Cores hoặc R_MAX_WORKERS)
}

impl Config {
    /// From_env nạp biến môi trường Fail-Close 100% (Zero Fallback)
    pub fn from_env() -> Result<Self> {
        let kafka_brokers = Self::get_required_env("KAFKA_BROKERS")?;
        let kafka_raw_topic = Self::get_required_env("KAFKA_RAW_TOPIC")?;
        let kafka_group_id = Self::get_required_env("KAFKA_GROUP_ID")?;
        let minio_endpoint = Self::get_required_env("MINIO_ENDPOINT")?;
        let minio_bucket = Self::get_required_env("MINIO_BUCKET")?;
        let minio_access_key = Self::get_required_env("MINIO_ACCESS_KEY")?;
        let minio_secret_key = Self::get_required_env("MINIO_SECRET_KEY")?;

        let batch_size = env::var("BATCH_SIZE")
            .ok()
            .and_then(|v| v.parse().ok())
            .unwrap_or(1000);

        let flush_interval_ms = env::var("FLUSH_INTERVAL_MS")
            .ok()
            .and_then(|v| v.parse().ok())
            .unwrap_or(1000);

        // Tự động phát hiện số CPU logical cores của hệ thống nếu không khai báo R_MAX_WORKERS
        let default_cpus = std::thread::available_parallelism()
            .map(|n| n.get())
            .unwrap_or(4);

        let r_max_workers = env::var("R_MAX_WORKERS")
            .ok()
            .and_then(|v| v.parse().ok())
            .unwrap_or(default_cpus);

        Ok(Self {
            kafka_brokers,
            kafka_raw_topic,
            kafka_group_id,
            minio_endpoint,
            minio_bucket,
            minio_access_key,
            minio_secret_key,
            batch_size,
            flush_interval_ms,
            r_max_workers,
        })
    }

    /// Helper get_required_env bắt buộc biến môi trường phải tồn tại
    fn get_required_env(key: &str) -> Result<String> {
        env::var(key).map_err(|_| {
            AppError::Config(format!(
                "Thiếu biến môi trường bắt buộc '{}' (Fail-Close Triggered)",
                key
            ))
        })
    }
}
