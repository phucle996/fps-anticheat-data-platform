use thiserror::Error;

/// AppError định nghĩa toàn bộ các mã lỗi chuẩn hóa của Rust Inference Service
#[derive(Error, Debug)]
pub enum AppError {
    #[error("Lỗi cấu hình biến môi trường (Fail-Close Triggered): {0}")]
    Config(String),

    #[error("Lỗi nạp mô hình ONNX Model: {0}")]
    ModelLoad(String),

    #[error("Lỗi verify SHA-256 Checksum: {0}")]
    ChecksumMismatch(String),

    #[error("Lỗi thực thi Unix Domain Socket IPC: {0}")]
    Ipc(String),

    #[error("Lỗi IO File System: {0}")]
    Io(#[from] std::io::Error),

    #[error("Lỗi Serde JSON: {0}")]
    Json(#[from] serde_json::Error),
}

pub type Result<T> = std::result::Result<T, AppError>;
