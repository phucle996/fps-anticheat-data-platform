use thiserror::Error;

/// Enum AppError định nghĩa tất cả các loại lỗi định dạng trong hệ thống Rust Processor
#[derive(Error, Debug)]
pub enum AppError {
    /// Lỗi cấu hình môi trường thiếu hoặc không hợp lệ (Fail-Close)
    #[error("Cấu hình không hợp lệ (Fail-Close Rule Violated): {0}")]
    Config(String),

    /// Lỗi kết nối hoặc thao tác Kafka Cluster
    #[error("Sự cố Kafka Driver: {0}")]
    Kafka(String),

    /// Lỗi giải mã hoặc mã hóa JSON payload
    #[error("Lỗi Deserialize/Serialize JSON: {0}")]
    Json(#[from] serde_json::Error),

    /// Lỗi định dạng mảng bộ nhớ Apache Arrow
    #[error("Lỗi Apache Arrow Memory Format: {0}")]
    Arrow(#[from] arrow::error::ArrowError),

    /// Lỗi ghi file cột Apache Parquet
    #[error("Lỗi Apache Parquet Storage: {0}")]
    Parquet(#[from] parquet::errors::ParquetError),

    /// Lỗi thao tác với MinIO S3 Object Storage
    #[error("Sự cố MinIO S3 Object Store: {0}")]
    Storage(#[from] object_store::Error),

    /// Lỗi hệ thống I/O chung
    #[error("Lỗi I/O Hệ thống: {0}")]
    Io(#[from] std::io::Error),
}

/// Type aliasResult chuẩn hóa cho toàn bộ dự án Rust Processor
pub type Result<T> = std::result::Result<T, AppError>;
