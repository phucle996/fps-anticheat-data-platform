mod app;
mod config;
mod domain;
mod error;
mod ingest;
mod storage;
mod transform;
mod transport;
mod worker;

use app::StreamProcessorApp;
use config::Config;
use error::Result;
use tracing::{info, warn};
use tracing_subscriber::{EnvFilter, layer::SubscriberExt, util::SubscriberInitExt};

#[tokio::main]
async fn main() -> Result<()> {
    // 1. Khởi tạo Structured JSON Logging chuẩn Cloud-Native
    let env_filter = EnvFilter::try_from_default_env().unwrap_or_else(|_| EnvFilter::new("info"));
    let json_formatting = tracing_subscriber::fmt::layer().json();

    tracing_subscriber::registry()
        .with(env_filter)
        .with(json_formatting)
        .init();

    info!("Khởi động tiến trình Rust Stream Processor Engine (Thin Entrypoint Active)...");

    // 2. Nạp cấu hình ứng dụng từ biến môi trường (Fail-Close 100%)
    let config = match Config::from_env() {
        Ok(cfg) => cfg,
        Err(err) => {
            warn!(
                "Dừng chương trình do lỗi nạp cấu hình (Fail-Close Triggered): {}",
                err
            );
            return Err(err);
        }
    };

    // 3. Khởi tạo và chạy StreamProcessorApp
    let mut app = StreamProcessorApp::new(config)?;
    app.run().await
}
