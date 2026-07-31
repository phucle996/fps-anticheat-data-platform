use thiserror::Error;

/// AppError phân loại các loại lỗi chính trong ứng dụng Rust Processor
#[derive(Error, Debug)]
#[allow(dead_code)]
pub enum AppError {
    #[error("Lỗi cấu hình: {0}")]
    Config(String),

    #[error("Lỗi Kafka: {0}")]
    Kafka(String),

    #[error("Lỗi Arrow: {0}")]
    Arrow(String),

    #[error("Lỗi Parquet: {0}")]
    Parquet(String),

    #[error("Lỗi S3 Storage: {0}")]
    Storage(String),

    #[error("Lỗi R worker: {0}")]
    Worker(String),
}

/// Result alias dùng chung cho toàn bộ dự án
pub type Result<T> = std::result::Result<T, AppError>;
