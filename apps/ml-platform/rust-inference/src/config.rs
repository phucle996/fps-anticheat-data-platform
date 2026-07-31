use crate::error::{AppError, Result};
use std::env;

/// Config lưu trữ cấu hình dịch vụ Rust Inference với nguyên tắc Fail-Close 100% (Zero Fallback)
#[derive(Debug, Clone)]
pub struct Config {
    pub model_dir: String, // Symlink/thư mục atomic `current` chứa ONNX bundle
    pub ipc_socket_path: String, // Đường dẫn file Unix Domain Socket IPC (vd: "/tmp/rust_inference.sock")
    pub policy_path: String,
    pub inference_session_pool_size: usize,
    pub model_reload_interval_seconds: u64,
    pub ipc_max_concurrency: usize,
    pub ipc_max_request_bytes: usize,
}

impl Config {
    /// From_env nạp biến môi trường Fail-Close 100% (Bắt buộc tất cả biến phải tồn tại)
    pub fn from_env() -> Result<Self> {
        let model_dir = Self::get_required_env("MODEL_DIR")?;
        let ipc_socket_path = Self::get_required_env("IPC_SOCKET_PATH")?;
        let policy_path = Self::get_required_env("POLICY_PATH")?;
        let inference_session_pool_size = Self::get_required_env("INFERENCE_SESSION_POOL_SIZE")?
            .parse::<usize>()
            .map_err(|err| {
                AppError::Config(format!("INFERENCE_SESSION_POOL_SIZE không hợp lệ: {err}"))
            })?;
        let model_reload_interval_seconds =
            Self::get_required_env("MODEL_RELOAD_INTERVAL_SECONDS")?
                .parse::<u64>()
                .map_err(|err| {
                    AppError::Config(format!("MODEL_RELOAD_INTERVAL_SECONDS không hợp lệ: {err}"))
                })?;
        let ipc_max_concurrency = Self::get_required_env("IPC_MAX_CONCURRENCY")?
            .parse::<usize>()
            .map_err(|err| AppError::Config(format!("IPC_MAX_CONCURRENCY không hợp lệ: {err}")))?;
        let ipc_max_request_bytes = Self::get_required_env("IPC_MAX_REQUEST_BYTES")?
            .parse::<usize>()
            .map_err(|err| {
                AppError::Config(format!("IPC_MAX_REQUEST_BYTES không hợp lệ: {err}"))
            })?;
        if inference_session_pool_size == 0
            || model_reload_interval_seconds == 0
            || ipc_max_concurrency == 0
            || ipc_max_request_bytes == 0
        {
            return Err(AppError::Config(
                "Pool size, reload interval và IPC limits phải lớn hơn 0".to_string(),
            ));
        }

        Ok(Self {
            model_dir,
            ipc_socket_path,
            policy_path,
            inference_session_pool_size,
            model_reload_interval_seconds,
            ipc_max_concurrency,
            ipc_max_request_bytes,
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
