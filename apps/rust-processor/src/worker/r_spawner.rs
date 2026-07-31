use crate::error::{AppError, Result};
use crate::storage::MinioWriter;
use serde::{Deserialize, Serialize};
use std::path::{Path, PathBuf};
use std::sync::Arc;
use std::time::Duration;
use tokio::process::Command;
use tokio::sync::Semaphore;
use tokio::time::timeout;
use tracing::info;

/// Artifact đã durable trên MinIO. Gold artifacts được dùng để phát `dataset.gold.ready`.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct UploadedArtifact {
    pub object_key: String,
    pub checksum_sha256: String,
    pub layer: String,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct RWorkerResult {
    pub manifest_key: String,
    pub artifacts: Vec<UploadedArtifact>,
}

/// Điều phối R subprocess với bounded concurrency và deadline.
#[derive(Clone)]
pub struct RWorkerSpawner {
    semaphore: Arc<Semaphore>,
    writer: Arc<MinioWriter>,
    timeout: Duration,
}

impl RWorkerSpawner {
    pub fn new(max_concurrency: usize, writer: Arc<MinioWriter>, timeout: Duration) -> Self {
        Self {
            semaphore: Arc::new(Semaphore::new(max_concurrency.max(1))),
            writer,
            timeout,
        }
    }

    /// Hoàn tất toàn bộ Bronze -> Silver/Gold trước khi caller được commit Kafka offset.
    pub async fn process_manifest(&self, manifest_key: &str) -> Result<RWorkerResult> {
        let _permit = self
            .semaphore
            .clone()
            .acquire_owned()
            .await
            .map_err(|err| AppError::Worker(format!("Semaphore R worker đã đóng: {}", err)))?;

        let temp_dir = tempfile::tempdir()
            .map_err(|err| AppError::Worker(format!("Không thể tạo temp directory: {}", err)))?;
        let temp_path = temp_dir.path();

        let local_manifest_path = self
            .writer
            .download_to_temp(manifest_key, temp_path)
            .await?;
        let bronze_key = Self::parse_bronze_key_from_manifest(&local_manifest_path)?;
        let local_bronze_path = self.writer.download_to_temp(&bronze_key, temp_path).await?;

        let (r_work_dir, r_script_path) = Self::resolve_runtime_paths();
        let mut command = Command::new("Rscript");
        command
            .arg(&r_script_path)
            .arg("--manifest")
            .arg(&local_manifest_path)
            .arg("--bronze")
            .arg(&local_bronze_path)
            .arg("--output-dir")
            .arg(temp_path)
            .current_dir(&r_work_dir)
            .kill_on_drop(true);

        let output = timeout(self.timeout, command.output())
            .await
            .map_err(|_| {
                AppError::Worker(format!(
                    "R worker quá deadline {} giây cho manifest '{}'",
                    self.timeout.as_secs(),
                    manifest_key
                ))
            })?
            .map_err(|err| AppError::Worker(format!("Không thể chạy Rscript: {}", err)))?;

        if !output.status.success() {
            return Err(AppError::Worker(format!(
                "Rscript thất bại cho manifest '{}': exit={:?}, stderr={}",
                manifest_key,
                output.status.code(),
                String::from_utf8_lossy(&output.stderr).trim()
            )));
        }

        let artifacts = Self::upload_r_outputs(&self.writer, temp_path, manifest_key).await?;
        info!(
            manifest_key = %manifest_key,
            artifacts = artifacts.len(),
            stdout = %String::from_utf8_lossy(&output.stdout).trim(),
            "R worker đã ghi durable toàn bộ Silver/Gold artifacts"
        );

        Ok(RWorkerResult {
            manifest_key: manifest_key.to_string(),
            artifacts,
        })
    }

    /// Container dùng /opt; local test/dev dùng đường dẫn tương đối từ repo root.
    pub fn resolve_runtime_paths() -> (PathBuf, PathBuf) {
        let container_script = PathBuf::from("/opt/r-processor/scripts/run_batch.R");
        if container_script.exists() {
            (PathBuf::from("/opt/r-processor"), container_script)
        } else {
            let work_dir = PathBuf::from(env!("CARGO_MANIFEST_DIR")).join("r-processor");
            let script = work_dir.join("scripts/run_batch.R");
            (work_dir, script)
        }
    }

    fn parse_bronze_key_from_manifest(manifest_path: &Path) -> Result<String> {
        let content = std::fs::read_to_string(manifest_path).map_err(|err| {
            AppError::Storage(format!(
                "Đọc manifest local '{}' thất bại: {}",
                manifest_path.display(),
                err
            ))
        })?;
        let json: serde_json::Value = serde_json::from_str(&content)
            .map_err(|err| AppError::Storage(format!("Parse manifest JSON thất bại: {}", err)))?;

        json["data_object_path"]
            .as_str()
            .filter(|value| !value.trim().is_empty())
            .map(str::to_string)
            .ok_or_else(|| {
                AppError::Storage(
                    "Manifest thiếu data_object_path; không thể chạy R projection".to_string(),
                )
            })
    }

    async fn upload_r_outputs(
        writer: &Arc<MinioWriter>,
        temp_dir: &Path,
        manifest_key: &str,
    ) -> Result<Vec<UploadedArtifact>> {
        let layers = [
            ("silver/players", "silver/players"),
            ("silver/matches", "silver/matches"),
            ("silver/player-match", "silver/player-match"),
            ("silver/kill-events", "silver/kill-events"),
            ("gold/player-match-features", "gold/player-match-features"),
        ];
        let mut artifacts = Vec::new();

        for (local_subdir, s3_prefix) in layers {
            let local_layer_dir = temp_dir.join(local_subdir);
            if !local_layer_dir.exists() {
                continue;
            }

            let entries = std::fs::read_dir(&local_layer_dir).map_err(|err| {
                AppError::Worker(format!(
                    "Không thể đọc R output directory '{}': {}",
                    local_layer_dir.display(),
                    err
                ))
            })?;

            for entry in entries {
                let file_path = entry
                    .map_err(|err| {
                        AppError::Worker(format!("Đọc R output entry thất bại: {}", err))
                    })?
                    .path();
                if !file_path.is_file() {
                    continue;
                }
                let file_name = file_path
                    .file_name()
                    .and_then(|name| name.to_str())
                    .ok_or_else(|| AppError::Worker("Tên R output không phải UTF-8".to_string()))?;
                let object_key = format!("{}/{}", s3_prefix, file_name);

                // Retry cùng batch ghi cùng key; MinIO versioning giữ lịch sử và logical head idempotent.
                let checksum_sha256 = writer.upload_file(&file_path, &object_key).await?;
                artifacts.push(UploadedArtifact {
                    object_key,
                    checksum_sha256,
                    layer: s3_prefix.to_string(),
                });
            }
        }

        if artifacts.is_empty() {
            return Err(AppError::Worker(format!(
                "Rscript thành công nhưng không tạo artifact cho manifest '{}'",
                manifest_key
            )));
        }
        Ok(artifacts)
    }
}
