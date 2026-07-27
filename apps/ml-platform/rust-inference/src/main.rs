use rust_inference::config::Config;
use rust_inference::error::Result;
use rust_inference::inference::OnnxInferenceEngine;
use rust_inference::ipc::UdsIpcServer;
use tracing::info;

#[tokio::main]
async fn main() -> Result<()> {
    // 1. Khởi tạo Structured JSON Logging
    tracing_subscriber::fmt::init();

    info!("Bắt đầu khởi chạy Dịch vụ Dedicated Rust Inference Engine...");

    // 2. Nạp cấu hình từ biến môi trường với cơ chế Fail-Close 100%
    let config = match Config::from_env() {
        Ok(cfg) => cfg,
        Err(err) => {
            eprintln!("[FAIL-CLOSE] Không thể khởi chạy Rust Inference Service: {}", err);
            return Err(err);
        }
    };

    // 3. Khởi tạo ONNX Inference Engine
    let engine = OnnxInferenceEngine::new(&config.model_dir)?;

    // 4. Khởi chạy UdsIpcServer lắng nghe trên Unix Domain Socket IPC
    let uds_server = UdsIpcServer::new(config.ipc_socket_path.clone(), engine);

    info!(
        socket_path = %config.ipc_socket_path,
        "Rust Inference Service đã khởi động thành công và sẵn sàng nhận IPC requests!"
    );

    uds_server.run().await
}
