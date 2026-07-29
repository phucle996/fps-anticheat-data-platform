use crate::config::Config;
use crate::error::{AppError, Result};
use crate::storage::MinioWriter;
use std::path::PathBuf;
use std::sync::Arc;
use tokio::process::Command;
use tokio::sync::Semaphore;
use tracing::{error, info, warn};

/// RWorkerSpawner điều phối các Rscript Subprocesses bất đồng bộ song song với Semaphore Concurrency Control
/// Luồng thực thi mỗi batch:
///   1. Tạo temp dir riêng cho batch
///   2. Download manifest JSON từ MinIO về temp dir
///   3. Download Bronze Parquet từ MinIO về temp dir (theo data_object_path trong manifest)
///   4. Gọi Rscript với local paths → R tạo Silver/Gold Parquet trong temp dir
///   5. Upload Silver/Gold từ temp dir lên MinIO
///   6. Cleanup temp dir
#[derive(Clone)]
pub struct RWorkerSpawner {
    semaphore: Arc<Semaphore>, // Giới hạn số lượng R subprocesses chạy song song (vd: max 4)
    script_path: String,       // Đường dẫn file script R entrypoint
    writer: Arc<MinioWriter>,  // MinIO client để download/upload trong bước pre/post R
}

impl RWorkerSpawner {
    /// New khởi tạo RWorkerSpawner với giới hạn số lượng worker song song và MinIO client
    pub fn new(max_concurrency: usize, writer: Arc<MinioWriter>) -> Self {
        let max_permits = if max_concurrency == 0 { 4 } else { max_concurrency };
        Self {
            semaphore: Arc::new(Semaphore::new(max_permits)),
            // Đường dẫn Rscript entrypoint tương ứng với cấu trúc subfolder
            script_path: "apps/rust-processor/r-processor/scripts/run_batch.R".to_string(),
            writer,
        }
    }

    /// Spawn_worker kích hoạt 1 Rscript Subprocess bất đồng bộ không gây nghẽn luồng chính
    /// manifest_key là S3 object key (không phải local path)
    pub fn spawn_worker(&self, manifest_key: String) {
        let sem = self.semaphore.clone();
        let script = self.script_path.clone();
        let writer = self.writer.clone();

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
                manifest_key = %manifest_key,
                "Bắt đầu R Worker: Download MinIO → Local Temp → Rscript → Upload kết quả"
            );

            // Bước 1: Tạo temp directory riêng cho batch này
            // Mỗi batch có temp dir riêng để tránh race condition khi chạy song song
            let temp_dir = match tempfile::tempdir() {
                Ok(d) => d,
                Err(err) => {
                    error!(
                        error = %err,
                        manifest_key = %manifest_key,
                        "Không thể tạo temp directory cho R Worker batch"
                    );
                    return;
                }
            };
            let temp_path = temp_dir.path();

            // Bước 2: Download manifest JSON từ MinIO về local temp
            // Không có local manifest → Rscript không thể chạy → Fail-Close
            let local_manifest_path = match writer.download_to_temp(&manifest_key, temp_path).await {
                Ok(p) => p,
                Err(err) => {
                    error!(
                        error = %err,
                        manifest_key = %manifest_key,
                        "Download manifest từ MinIO thất bại — Bỏ qua batch này"
                    );
                    return;
                }
            };

            // Bước 3: Đọc manifest JSON để lấy data_object_path (Bronze Parquet key)
            // Parse manifest cục bộ để lấy bronze_key — không cần gọi MinIO thêm
            let bronze_key = match Self::parse_bronze_key_from_manifest(&local_manifest_path) {
                Ok(k) => k,
                Err(err) => {
                    error!(
                        error = %err,
                        manifest_key = %manifest_key,
                        "Parse manifest JSON thất bại — Không lấy được Bronze Parquet key"
                    );
                    return;
                }
            };

            // Bước 4: Download Bronze Parquet từ MinIO về local temp (cùng thư mục với manifest)
            // R silver_preprocessor đọc Bronze theo data_object_path trong manifest,
            // nên phải ghi file với cùng basename để R tìm đúng
            let local_bronze_path = match writer.download_to_temp(&bronze_key, temp_path).await {
                Ok(p) => p,
                Err(err) => {
                    error!(
                        error = %err,
                        bronze_key = %bronze_key,
                        "Download Bronze Parquet từ MinIO thất bại — Bỏ qua batch này"
                    );
                    return;
                }
            };

            info!(
                manifest_local = %local_manifest_path.display(),
                bronze_local = %local_bronze_path.display(),
                "Download hoàn tất — Khởi chạy Rscript Subprocess..."
            );

            // Bước 5: Khởi chạy Rscript Subprocess với local paths
            // --manifest = đường dẫn LOCAL file manifest (đã download)
            // --bronze   = đường dẫn LOCAL file Bronze Parquet (đã download)
            // --output   = thư mục temp để R ghi Silver/Gold output
            // Working directory đặt tại r-processor để source() đúng relative paths
            let output = Command::new("Rscript")
                .arg(&script)
                .arg("--manifest")
                .arg(&local_manifest_path)
                .arg("--bronze")
                .arg(&local_bronze_path)
                .arg("--output-dir")
                .arg(temp_path)
                .current_dir("apps/rust-processor/r-processor")
                .output()
                .await;

            match output {
                Ok(out) if out.status.success() => {
                    info!(
                        manifest_key = %manifest_key,
                        stdout = %String::from_utf8_lossy(&out.stdout).trim(),
                        "Rscript Subprocess hoàn tất thành công — Bắt đầu upload Silver/Gold lên MinIO"
                    );

                    // Bước 6: Upload Silver/Gold Parquet từ temp dir lên MinIO
                    // R ghi output vào temp_path/silver/ và temp_path/gold/
                    if let Err(err) = Self::upload_r_outputs(&writer, temp_path, &manifest_key).await {
                        error!(
                            error = %err,
                            manifest_key = %manifest_key,
                            "Upload Silver/Gold output lên MinIO thất bại"
                        );
                    }
                    // temp_dir sẽ tự động xóa khi drop (RAII)
                }
                Ok(out) => {
                    warn!(
                        manifest_key = %manifest_key,
                        exit_code = ?out.status.code(),
                        stderr = %String::from_utf8_lossy(&out.stderr).trim(),
                        "Rscript Subprocess thất bại — Dữ liệu Silver/Gold không được tạo cho batch này"
                    );
                }
                Err(err) => {
                    // Nếu môi trường chưa cài Rscript CLI, log warning và tiếp tục
                    warn!(
                        manifest_key = %manifest_key,
                        error = %err,
                        "Không thể khởi chạy Rscript CLI (Rscript chưa được cài đặt hoặc thiếu PATH)"
                    );
                }
            }
            // temp_dir drop ở đây → tự động xóa toàn bộ temp files (RAII cleanup)
        });
    }

    /// Parse_bronze_key_from_manifest đọc file manifest JSON local và trả về data_object_path
    /// data_object_path là S3 object key của Bronze Parquet file cần download
    fn parse_bronze_key_from_manifest(manifest_path: &PathBuf) -> Result<String> {
        let content = std::fs::read_to_string(manifest_path).map_err(|e| {
            AppError::Storage(format!(
                "Đọc manifest local file '{}' thất bại: {}",
                manifest_path.display(),
                e
            ))
        })?;

        // Parse JSON để lấy trường data_object_path
        let json: serde_json::Value = serde_json::from_str(&content).map_err(|e| {
            AppError::Storage(format!("Parse manifest JSON thất bại: {}", e))
        })?;

        json["data_object_path"]
            .as_str()
            .map(|s| s.to_string())
            .ok_or_else(|| {
                AppError::Storage(
                    "Manifest JSON thiếu trường 'data_object_path' — không thể download Bronze Parquet".to_string()
                )
            })
    }

    /// Upload_r_outputs quét temp_dir để tìm Silver/Gold Parquet files và upload lên MinIO
    /// R ghi output vào cấu trúc: <temp_dir>/silver/<layer>/*.parquet và <temp_dir>/gold/**/*.parquet
    async fn upload_r_outputs(
        writer: &Arc<MinioWriter>,
        temp_dir: &std::path::Path,
        manifest_key: &str,
    ) -> Result<()> {
        let layers = [
            ("silver/players", "silver/players"),
            ("silver/matches", "silver/matches"),
            ("silver/player-match", "silver/player-match"),
            ("silver/kill-events", "silver/kill-events"),
            ("gold/player-match-features", "gold/player-match-features"),
        ];

        let mut upload_count = 0usize;

        for (local_subdir, s3_prefix) in &layers {
            let local_layer_dir = temp_dir.join(local_subdir);
            if !local_layer_dir.exists() {
                continue; // Layer này R chưa tạo ra — bỏ qua
            }

            // Duyệt tất cả file trong layer directory (không recursive)
            let entries = match std::fs::read_dir(&local_layer_dir) {
                Ok(e) => e,
                Err(err) => {
                    warn!(
                        error = %err,
                        dir = %local_layer_dir.display(),
                        "Không thể đọc thư mục R output layer — bỏ qua"
                    );
                    continue;
                }
            };

            for entry in entries.flatten() {
                let file_path = entry.path();
                if !file_path.is_file() {
                    continue;
                }

                // Lấy tên file để tạo S3 key: <s3_prefix>/<filename>
                let file_name = match file_path.file_name().and_then(|n| n.to_str()) {
                    Some(n) => n.to_string(),
                    None => continue,
                };

                let s3_key = format!("{}/{}", s3_prefix, file_name);

                match writer.upload_file(&file_path, &s3_key).await {
                    Ok(checksum) => {
                        info!(
                            local_file = %file_path.display(),
                            s3_key = %s3_key,
                            checksum = %checksum,
                            "Đã upload R output lên MinIO thành công"
                        );
                        upload_count += 1;
                    }
                    Err(err) => {
                        warn!(
                            error = %err,
                            local_file = %file_path.display(),
                            "Upload R output file thất bại — bỏ qua file này"
                        );
                    }
                }
            }
        }

        if upload_count == 0 {
            warn!(
                manifest_key = %manifest_key,
                "R Subprocess không tạo ra bất kỳ Silver/Gold file nào để upload — kiểm tra Rscript log"
            );
        } else {
            info!(
                upload_count = upload_count,
                manifest_key = %manifest_key,
                "Đã upload tổng {} file Silver/Gold từ R output lên MinIO thành công",
                upload_count
            );
        }

        Ok(())
    }
}
