use crate::error::{AppError, Result};
use crate::storage::MinioWriter;
use crate::transform::{NativeGoldFeatureGenerator, NativeSilverPreprocessor, ParquetSerializer};
use serde::{Deserialize, Serialize};
use std::path::{Path, PathBuf};
use std::sync::Arc;
use std::time::Duration;
use tokio::sync::Semaphore;
use tracing::info;

/// Artifact đã durable trên MinIO. Gold artifacts được dùng để phát `dataset.gold.ready`.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct UploadedArtifact {
    pub object_key: String,
    pub checksum_sha256: String,
    pub layer: String,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct NativeWorkerResult {
    pub manifest_key: String,
    pub artifacts: Vec<UploadedArtifact>,
}

/// Native Worker Spawner điều phối Silver & Gold transformations hoàn toàn trong Rust
#[derive(Clone)]
pub struct NativeWorkerSpawner {
    semaphore: Arc<Semaphore>,
    writer: Arc<MinioWriter>,
    timeout: Duration,
}

impl NativeWorkerSpawner {
    pub fn new(max_concurrency: usize, writer: Arc<MinioWriter>, timeout: Duration) -> Self {
        Self {
            semaphore: Arc::new(Semaphore::new(max_concurrency.max(1))),
            writer,
            timeout,
        }
    }

    /// Hoàn tất toàn bộ Bronze -> Silver/Gold hoàn toàn trong Rust trước khi caller commit Kafka offset
    pub async fn process_manifest(&self, manifest_key: &str) -> Result<NativeWorkerResult> {
        let _permit = self
            .semaphore
            .clone()
            .acquire_owned()
            .await
            .map_err(|err| AppError::Worker(format!("Semaphore worker đã đóng: {}", err)))?;

        let temp_dir = tempfile::tempdir()
            .map_err(|err| AppError::Worker(format!("Không thể tạo temp directory: {}", err)))?;
        let temp_path = temp_dir.path();

        // 1. Download Manifest JSON từ MinIO
        let local_manifest_path = self
            .writer
            .download_to_temp(manifest_key, temp_path)
            .await?;
        let bronze_key = Self::parse_bronze_key_from_manifest(&local_manifest_path)?;

        // 2. Download Bronze Parquet từ MinIO
        let local_bronze_path = self.writer.download_to_temp(&bronze_key, temp_path).await?;

        // 3. Đọc Bronze Parquet bytes và deserialize thành RecordBatches
        let bronze_bytes = tokio::fs::read(&local_bronze_path).await.map_err(|e| {
            AppError::Worker(format!("Đọc Bronze Parquet file thất bại: {}", e))
        })?;
        let bronze_batches = ParquetSerializer::read_parquet_bytes(&bronze_bytes)?;

        if bronze_batches.is_empty() {
            return Err(AppError::Worker(format!(
                "Bronze Parquet rỗng cho manifest '{}'",
                manifest_key
            )));
        }

        // Extract batch_id từ manifest key hoặc filename
        let batch_id = Path::new(manifest_key)
            .file_stem()
            .and_then(|s| s.to_str())
            .unwrap_or("batch");

        // 4. Biến đổi Silver Entities trong Native Rust
        let (silver_ke_batch, silver_pm_batch) =
            NativeSilverPreprocessor::process_silver(&bronze_batches)?;

        // 5. Trích xuất Gold Feature Matrix trong Native Rust
        let gold_batch = NativeGoldFeatureGenerator::generate_gold(&silver_ke_batch, &silver_pm_batch)?;

        // 6. Serialize kết quả sang Parquet bytes và ghi ra temp directory
        let silver_ke_bytes = ParquetSerializer::record_batch_to_parquet_bytes(&silver_ke_batch)?;
        let silver_pm_bytes = ParquetSerializer::record_batch_to_parquet_bytes(&silver_pm_batch)?;
        let gold_bytes = ParquetSerializer::record_batch_to_parquet_bytes(&gold_batch)?;

        let silver_ke_dir = temp_path.join("silver/kill-events");
        let silver_pm_dir = temp_path.join("silver/player-match");
        let gold_dir = temp_path.join("gold/player-match-features");

        tokio::fs::create_dir_all(&silver_ke_dir).await?;
        tokio::fs::create_dir_all(&silver_pm_dir).await?;
        tokio::fs::create_dir_all(&gold_dir).await?;

        let ke_file = silver_ke_dir.join(format!("kill_events_{}.parquet", batch_id));
        let pm_file = silver_pm_dir.join(format!("player_match_{}.parquet", batch_id));
        let gold_file = gold_dir.join(format!("gold_features_{}.parquet", batch_id));

        tokio::fs::write(&ke_file, silver_ke_bytes).await?;
        tokio::fs::write(&pm_file, silver_pm_bytes).await?;
        tokio::fs::write(&gold_file, gold_bytes).await?;

        // 7. Upload tất cả Silver & Gold artifacts lên MinIO
        let artifacts = Self::upload_outputs(&self.writer, temp_path, manifest_key).await?;

        info!(
            manifest_key = %manifest_key,
            artifacts = artifacts.len(),
            "Native Rust worker đã ghi durable toàn bộ Silver/Gold artifacts"
        );

        Ok(NativeWorkerResult {
            manifest_key: manifest_key.to_string(),
            artifacts,
        })
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
                    "Manifest thiếu data_object_path; không thể chạy Rust projection".to_string(),
                )
            })
    }

    async fn upload_outputs(
        writer: &Arc<MinioWriter>,
        temp_dir: &Path,
        manifest_key: &str,
    ) -> Result<Vec<UploadedArtifact>> {
        let layers = [
            ("silver/kill-events", "silver/kill-events"),
            ("silver/player-match", "silver/player-match"),
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
                    "Không thể đọc output directory '{}': {}",
                    local_layer_dir.display(),
                    err
                ))
            })?;

            for entry in entries {
                let file_path = entry
                    .map_err(|err| {
                        AppError::Worker(format!("Đọc output entry thất bại: {}", err))
                    })?
                    .path();
                if !file_path.is_file() {
                    continue;
                }
                let file_name = file_path
                    .file_name()
                    .and_then(|name| name.to_str())
                    .ok_or_else(|| AppError::Worker("Tên output không phải UTF-8".to_string()))?;
                let object_key = format!("{}/{}", s3_prefix, file_name);

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
                "Native Rust worker không tạo artifact nào cho manifest '{}'",
                manifest_key
            )));
        }
        Ok(artifacts)
    }
}
