use std::sync::Arc;
use tokio::process::Command;
use tokio::sync::Semaphore;
use tracing::{error, info, warn};

/// RWorkerSpawner điều phối các Rscript Subprocesses bất đồng bộ song song với Semaphore Concurrency Control
#[derive(Clone)]
pub struct RWorkerSpawner {
    semaphore: Arc<Semaphore>, // Giới hạn số lượng R subprocesses chạy song song (vd: max 4)
    script_path: String,       // Đường dẫn file script R entrypoint
}

impl RWorkerSpawner {
    /// New khởi tạo RWorkerSpawner với giới hạn số lượng worker song song
    pub fn new(max_concurrency: usize) -> Self {
        let max_permits = if max_concurrency == 0 { 4 } else { max_concurrency };
        Self {
            semaphore: Arc::new(Semaphore::new(max_permits)),
            // Cập nhật đường dẫn Rscript entrypoint tương ứng với cấu trúc subfolder mới
            script_path: "apps/rust-processor/r-processor/scripts/run_batch.R".to_string(),
        }
    }

    /// Spawn_worker kích hoạt 1 Rscript Subprocess bất đồng bộ không gây nghẽn luồng chính
    pub fn spawn_worker(&self, manifest_path: String) {
        let sem = self.semaphore.clone();
        let script = self.script_path.clone();

        // Spawn async background task để thực thi subprocess R
        tokio::spawn(async move {
            // Lấy permit từ Semaphore để đảm bảo không bị cạn kiệt CPU/RAM (Concurrency Control)
            let _permit = match sem.acquire_owned().await {
                Ok(p) => p,
                Err(err) => {
                    error!(error = %err, "Lỗi acquire Semaphore permit cho R Worker");
                    return;
                }
            };

            info!(
                manifest = %manifest_path,
                "Khởi chạy Rscript Async Subprocess Worker từ subfolder apps/rust-processor/r-processor..."
            );

            // Khởi chạy Rscript Subprocess với Working Directory đặt tại apps/rust-processor/r-processor
            // Giúp Rscript tự động nạp các module tương đối (R/config.R, R/silver_preprocessor.R...) an toàn
            let output = Command::new("Rscript")
                .arg(&script)
                .arg("--manifest")
                .arg(&manifest_path)
                .current_dir("apps/rust-processor/r-processor")
                .output()
                .await;

            match output {
                Ok(out) => {
                    if out.status.success() {
                        info!(
                            manifest = %manifest_path,
                            stdout = %String::from_utf8_lossy(&out.stdout).trim(),
                            "Rscript Subprocess Worker xử lý hoàn tất thành công!"
                        );
                    } else {
                        warn!(
                            manifest = %manifest_path,
                            exit_code = ?out.status.code(),
                            stderr = %String::from_utf8_lossy(&out.stderr).trim(),
                            "Rscript Subprocess Worker vi phạm lỗi nhưng Rust Ingestor vẫn hoạt động an toàn"
                        );
                    }
                }
                Err(err) => {
                    // Nếu môi trường test chưa cài Rscript CLI, log warning và tiếp tục trơn tru
                    warn!(
                        manifest = %manifest_path,
                        error = %err,
                        "Không thể khởi chạy Rscript CLI (Rscript chưa được cài đặt hoặc thiếu PATH)"
                    );
                }
            }
        });
    }
}
