use crate::error::{AppError, Result};
use std::env;

/// Config lưu trữ cấu hình dịch vụ Rust Inference với nguyên tắc Fail-Close 100% (Zero Fallback)
#[derive(Debug, Clone)]
pub struct Config {
    pub model_dir: String,       // Đường dẫn thư mục shared chứa ONNX bundle (vd: "models/v1")
    pub ipc_socket_path: String, // Đường dẫn file Unix Domain Socket IPC (vd: "/tmp/rust_inference.sock")
    pub minio_endpoint: String,  // Endpoint MinIO S3 (vd: "http://localhost:9000")
    pub minio_bucket_model: String, // Bucket chứa ONNX model (vd: "pubg-models")
}

impl Config {
    /// From_env nạp biến môi trường Fail-Close 100% (Bắt buộc tất cả biến phải tồn tại)
    pub fn from_env() -> Result<Self> {
        let model_dir = Self::get_required_env("MODEL_DIR")?;
        let ipc_socket_path = Self::get_required_env("IPC_SOCKET_PATH")?;
        let minio_endpoint = Self::get_required_env("MINIO_ENDPOINT")?;
        let minio_bucket_model = Self::get_required_env("MINIO_BUCKET_MODEL")?;

        Ok(Self {
            model_dir,
            ipc_socket_path,
            minio_endpoint,
            minio_bucket_model,
        })
    }

    /// Helper get_required_env bắt buộc biến môi trường không được rỗng
    fn get_required_env(key: &str) -> Result<String> {
        env::var(key).map_err(|_| {
            AppError::Config(format!(
                "Thiếu biến môi trường bắt buộc '{}' (Fail-Close Triggered)",
                key
            ))
        })
    }
}
