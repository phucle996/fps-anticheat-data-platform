use super::manifest::BatchManifest;
use crate::config::Config;
use crate::error::{AppError, Result};
use crate::transform::InvalidEnvelopeRecord;
use chrono::{DateTime, Utc};
use object_store::aws::AmazonS3Builder;
use object_store::path::Path as ObjectPath;
use object_store::ObjectStore;
use std::sync::Arc;
use tracing::{info, warn};

/// MinioWriter quản lý kết nối Cloud-Native MinIO S3 Object Storage và upload dữ liệu Bronze Layer
pub struct MinioWriter {
    store: Arc<dyn ObjectStore>, // Interface ObjectStore thread-safe
    bucket: String,              // Tên S3 Bucket (fps-anticheat-datalake)
}

impl MinioWriter {
    /// New khởi tạo MinioWriter từ Config (nạp credentials Fail-Close 100%)
    pub fn new(config: &Config) -> Result<Self> {
        // Cấu hình AmazonS3Builder kết nối tới local/cloud MinIO S3 Server
        let store = AmazonS3Builder::new()
            .with_endpoint(&config.minio_endpoint)
            .with_bucket_name(&config.minio_bucket)
            .with_access_key_id(&config.minio_access_key)
            .with_secret_access_key(&config.minio_secret_key)
            .with_region("us-east-1")
            .with_allow_http(true) // Cho phép HTTP kết nối nội bộ Docker/MinIO
            .build()
            .map_err(|e| AppError::Storage(format!("Khởi tạo AmazonS3 ObjectStore thất bại: {}", e)))?;

        info!(
            endpoint = %config.minio_endpoint,
            bucket = %config.minio_bucket,
            "Đã khởi tạo thành công MinioWriter ObjectStore Client"
        );

        Ok(Self {
            store: Arc::new(store),
            bucket: config.minio_bucket.clone(),
        })
    }

    /// Generate_bronze_path sinh đường dẫn Hive Partitioning chuẩn cho file Parquet hợp lệ
    pub fn generate_bronze_path(batch_id: &str, ingest_time_str: &str) -> String {
        let (year, month, day) = Self::parse_date_or_now(ingest_time_str);
        format!(
            "bronze/player-stat/year={}/month={:02}/day={:02}/pubg_player_stat_{}.parquet",
            year, month, day, batch_id
        )
    }

    /// Generate_invalid_path sinh đường dẫn Hive Partitioning chuẩn cho bản ghi vi phạm
    pub fn generate_invalid_path(batch_id: &str, ingest_time_str: &str) -> String {
        let (year, month, day) = Self::parse_date_or_now(ingest_time_str);
        format!(
            "bronze/invalid/year={}/month={:02}/day={:02}/pubg_invalid_{}.json",
            year, month, day, batch_id
        )
    }

    /// Generate_manifest_path sinh đường dẫn Hive Partitioning cho Batch Manifest JSON
    pub fn generate_manifest_path(batch_id: &str, time_str: &str) -> String {
        let (year, month, day) = Self::parse_date_or_now(time_str);
        format!(
            "manifests/year={}/month={:02}/day={:02}/manifest_{}.json",
            year, month, day, batch_id
        )
    }

    /// Compute_sha256 tính toán mã băm SHA-256 checksum của file bytes
    pub fn compute_sha256(bytes: &[u8]) -> String {
        use sha2::{Digest, Sha256};
        let mut hasher = Sha256::new();
        hasher.update(bytes);
        format!("{:x}", hasher.finalize())
    }

    /// Upload_parquet upload file Parquet nén Zstandard lên MinIO S3 và xác thực checksum
    pub async fn upload_parquet(&self, path_str: &str, bytes: Vec<u8>) -> Result<String> {
        let checksum = Self::compute_sha256(&bytes);
        let object_path = ObjectPath::from(path_str);

        self.store
            .put(&object_path, bytes.into())
            .await
            .map_err(|e| AppError::Storage(format!("Upload Parquet file lên MinIO path {} thất bại: {}", path_str, e)))?;

        info!(
            bucket = %self.bucket,
            path = %path_str,
            checksum = %checksum,
            "Đã upload thành công Parquet Bronze file lên MinIO S3 Object Storage"
        );

        Ok(checksum)
    }

    /// Upload_manifest upload BatchManifest JSON lên MinIO S3
    pub async fn upload_manifest(&self, path_str: &str, manifest: &BatchManifest) -> Result<()> {
        let json_bytes = serde_json::to_vec_pretty(manifest)
            .map_err(|e| AppError::Storage(format!("Mã hóa BatchManifest JSON thất bại: {}", e)))?;

        let object_path = ObjectPath::from(path_str);
        self.store
            .put(&object_path, json_bytes.into())
            .await
            .map_err(|e| AppError::Storage(format!("Upload BatchManifest JSON lên MinIO path {} thất bại: {}", path_str, e)))?;

        info!(
            bucket = %self.bucket,
            path = %path_str,
            batch_id = %manifest.batch_id,
            "Đã upload thành công Batch Manifest Audit Log lên MinIO S3"
        );

        Ok(())
    }

    /// Upload_invalid_records upload danh sách bản ghi vi phạm dưới dạng JSON lên MinIO S3
    pub async fn upload_invalid_records(&self, path_str: &str, records: &[InvalidEnvelopeRecord]) -> Result<()> {
        if records.is_empty() {
            return Ok(());
        }

        let json_bytes = serde_json::to_vec_pretty(records)
            .map_err(|e| AppError::Storage(format!("Mã hóa Invalid Records JSON thất bại: {}", e)))?;

        let object_path = ObjectPath::from(path_str);
        self.store
            .put(&object_path, json_bytes.into())
            .await
            .map_err(|e| AppError::Storage(format!("Upload Invalid Records JSON lên MinIO path {} thất bại: {}", path_str, e)))?;

        warn!(
            bucket = %self.bucket,
            path = %path_str,
            invalid_count = records.len(),
            "Đã ghi danh sách bản ghi vi phạm Data Quality sang bronze/invalid/"
        );

        Ok(())
    }

    /// Helper parse_date_or_now giải mã năm, tháng, ngày từ chuỗi RFC3339
    fn parse_date_or_now(time_str: &str) -> (i32, u32, u32) {
        if let Ok(dt) = DateTime::parse_from_rfc3339(time_str) {
            use chrono::Datelike;
            (dt.year(), dt.month(), dt.day())
        } else {
            use chrono::Datelike;
            let now = Utc::now();
            (now.year(), now.month(), now.day())
        }
    }
}
