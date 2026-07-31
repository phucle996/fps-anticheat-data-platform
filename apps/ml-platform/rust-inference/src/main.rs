use rust_inference::config::Config;
use rust_inference::error::Result;
use rust_inference::inference::OnnxInferenceEngine;
use rust_inference::ipc::UdsIpcServer;
use std::time::Duration;
use tokio::sync::watch;
use tracing::{error, info, warn};

#[tokio::main]
async fn main() -> Result<()> {
    // 1. Khởi tạo Structured JSON Logging
    tracing_subscriber::fmt::init();

    info!("Bắt đầu khởi chạy Dịch vụ Dedicated Rust Inference Engine...");

    // 2. Nạp cấu hình từ biến môi trường với cơ chế Fail-Close 100%
    let config = match Config::from_env() {
        Ok(cfg) => cfg,
        Err(err) => {
            eprintln!(
                "[FAIL-CLOSE] Không thể khởi chạy Rust Inference Service: {}",
                err
            );
            return Err(err);
        }
    };

    // 3. Khởi tạo ONNX Inference Engine
    let engine =
        OnnxInferenceEngine::new_with_pool(&config.model_dir, config.inference_session_pool_size)?;

    let uds_server = UdsIpcServer::new(
        config.ipc_socket_path.clone(),
        engine.clone(),
        &config.policy_path,
        config.ipc_max_concurrency,
        config.ipc_max_request_bytes,
    )?;

    info!(
        socket_path = %config.ipc_socket_path,
        "Rust Inference Service đã khởi động thành công và sẵn sàng nhận IPC requests!"
    );

    let (shutdown_tx, mut shutdown_rx) = watch::channel(false);
    let reload_engine = engine.clone();
    let reload_dir = config.model_dir.clone();
    let reload_interval = config.model_reload_interval_seconds;
    let reload_task = tokio::spawn(async move {
        let mut ticker = tokio::time::interval(Duration::from_secs(reload_interval));
        ticker.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Skip);
        loop {
            tokio::select! {
                _ = ticker.tick() => {
                    match reload_engine.hot_swap(&reload_dir) {
                        Ok(true) => info!("Đã activate model bundle mới"),
                        Ok(false) => {}
                        Err(err) => {
                            // Model lỗi không được thay model tốt đang phục vụ.
                            warn!(error = %err, "Bỏ qua model candidate không hợp lệ");
                        }
                    }
                }
                changed = shutdown_rx.changed() => {
                    if changed.is_err() || *shutdown_rx.borrow() {
                        break;
                    }
                }
            }
        }
    });

    let shutdown_sender = shutdown_tx.clone();
    let server_result = uds_server
        .run_until_shutdown(async move {
            wait_for_shutdown_signal().await;
            let _ = shutdown_sender.send(true);
        })
        .await;
    let _ = shutdown_tx.send(true);
    if let Err(err) = reload_task.await {
        error!(error = %err, "Model reload task kết thúc bất thường");
    }
    server_result
}

async fn wait_for_shutdown_signal() {
    #[cfg(unix)]
    {
        let mut terminate =
            tokio::signal::unix::signal(tokio::signal::unix::SignalKind::terminate())
                .expect("Không đăng ký được SIGTERM handler");
        tokio::select! {
            _ = tokio::signal::ctrl_c() => {}
            _ = terminate.recv() => {}
        }
    }
    #[cfg(not(unix))]
    {
        let _ = tokio::signal::ctrl_c().await;
    }
}
