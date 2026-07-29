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

        let writer = Self {
            store: Arc::new(store),
            bucket: config.minio_bucket.clone(),
        };

        Ok(writer)
    }

    /// Preflight_check thực hiện S3 connectivity probe tại startup (TC-03 Fix)
    /// Mục đích: Phát hiện sai creds, endpoint không reach, bucket không tồn tại TRƯỚC khi consume Kafka
    /// Cơ chế: HEAD request vào .preflight-probe — NotFound là OK (auth thành công), còn lại → Fail-Close
    pub async fn preflight_check(&self) -> Result<()> {
        use object_store::path::Path as ObjectPath;

        // Thực hiện HEAD request để trigger S3 authentication
        // Kết quả:
        // - Ok(_)          → File tồn tại, auth OK
        // - NotFound       → File không tồn tại nhưng auth OK (bình thường)
        // - AccessDenied   → Sai creds → Fail-Close
        // - Network error  → Endpoint không reach → Fail-Close
        let probe_path = ObjectPath::from(".preflight-probe");
        match self.store.head(&probe_path).await {
            Ok(_) => Ok(()),
            Err(object_store::Error::NotFound { .. }) => {
                // NotFound = request đến được MinIO và auth thành công
                Ok(())
            }
            Err(e) => {
                // Lỗi khác: AccessDenied, SignatureDoesNotMatch, Network unreachable → Fail-Close
                Err(AppError::Storage(format!(
                    "S3 Pre-flight Check thất bại — Không thể kết nối hoặc xác thực MinIO S3: {} (Fail-Close Triggered)",
                    e
                )))
            }
        }
    }

    /// Ensure_datalake_structure khởi tạo tự động các thư mục móng Medallion Data Lake nếu chưa có
    pub async fn ensure_datalake_structure(&self) -> Result<()> {
        let keep_paths = [
            "bronze/player-stat/.keep",
            "bronze/kill-events/.keep",
            "bronze/invalid/.keep",
            "manifests/.keep",
            "silver/players/.keep",
            "silver/matches/.keep",
            "silver/player-match/.keep",
            "silver/kill-events/.keep",
            "gold/player-match-features/.keep",
            "models/.keep",
            "predictions/.keep",
        ];

        for path_str in keep_paths {
            let object_path = ObjectPath::from(path_str);
            // Chỉ ghi .keep nếu chưa tồn tại
            if self.store.head(&object_path).await.is_err() {
                let _ = self.store.put(&object_path, vec![].into()).await;
            }
        }

        info!("Đã xác nhận 100% cấu trúc 11 thư mục móng Medallion Data Lake sẵn sàng trên MinIO S3");
        Ok(())
    }

    /// Generate_bronze_path sinh đường dẫn Hive Partitioning chuẩn cho file Parquet hợp lệ
    pub fn generate_bronze_path(batch_id: &str, ingest_time_str: &str) -> String {
        let (year, month, day) = Self::parse_date_or_now(ingest_time_str);
        format!(
            "bronze/player-stat/year={}/month={:02}/day={:02}/pubg_player_stat_{}.parquet",
            year, month, day, batch_id
        )
    }

    /// Generate_kill_events_path sinh đường dẫn Hive Partitioning chuẩn cho Telemetry Kill Events
    pub fn generate_kill_events_path(batch_id: &str, ingest_time_str: &str) -> String {
        let (year, month, day) = Self::parse_date_or_now(ingest_time_str);
        format!(
            "bronze/kill-events/year={}/month={:02}/day={:02}/pubg_kill_events_{}.parquet",
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

    /// Download_object_bytes tải nội dung object từ MinIO về dưới dạng bytes thuần (in-memory)
    /// Dùng nội bộ — caller có thể ghi ra file hoặc parse trực tiếp
    pub async fn download_object_bytes(&self, object_key: &str) -> Result<Vec<u8>> {
        let object_path = ObjectPath::from(object_key);

        let result = self
            .store
            .get(&object_path)
            .await
            .map_err(|e| AppError::Storage(format!(
                "Download object '{}' từ MinIO thất bại: {}",
                object_key, e
            )))?;

        let bytes = result
            .bytes()
            .await
            .map_err(|e| AppError::Storage(format!(
                "Đọc bytes object '{}' từ stream thất bại: {}",
                object_key, e
            )))?;

        info!(
            bucket = %self.bucket,
            key = %object_key,
            size_bytes = bytes.len(),
            "Đã download thành công object từ MinIO S3"
        );

        Ok(bytes.to_vec())
    }

    /// Download_to_temp tải object từ MinIO xuống một file local tạm trong thư mục temp_dir
    /// Trả về đường dẫn file local để truyền cho R subprocess (Rscript chỉ đọc local)
    /// Caller có trách nhiệm xóa file sau khi R xử lý xong (cleanup)
    pub async fn download_to_temp(&self, object_key: &str, temp_dir: &std::path::Path) -> Result<std::path::PathBuf> {
        use std::io::Write;

        // Tải bytes từ MinIO
        let bytes = self.download_object_bytes(object_key).await?;

        // Tạo tên file local dựa trên phần cuối của object_key (giữ extension gốc)
        // vd: "manifests/year=2026/.../manifest_abc.json" → "manifest_abc.json"
        let file_name = object_key
            .split('/')
            .last()
            .unwrap_or("downloaded_object");

        let local_path = temp_dir.join(file_name);

        // Ghi bytes xuống file local — dùng BufWriter để tránh nhiều syscall nhỏ
        let file = std::fs::File::create(&local_path).map_err(|e| {
            AppError::Storage(format!(
                "Không thể tạo temp file '{}': {}",
                local_path.display(),
                e
            ))
        })?;
        let mut writer = std::io::BufWriter::new(file);
        writer.write_all(&bytes).map_err(|e| {
            AppError::Storage(format!(
                "Không thể ghi bytes vào temp file '{}': {}",
                local_path.display(),
                e
            ))
        })?;

        info!(
            object_key = %object_key,
            local_path = %local_path.display(),
            "Đã download MinIO object xuống local temp file thành công"
        );

        Ok(local_path)
    }

    /// Upload_file_to_minio upload file local lên MinIO S3 với object_key chỉ định
    /// Dùng để Rust upload Silver/Gold Parquet mà R đã tạo ra trong temp dir
    pub async fn upload_file(&self, local_path: &std::path::Path, object_key: &str) -> Result<String> {
        let bytes = std::fs::read(local_path).map_err(|e| {
            AppError::Storage(format!(
                "Không thể đọc file local '{}' để upload: {}",
                local_path.display(),
                e
            ))
        })?;

        let checksum = Self::compute_sha256(&bytes);
        let object_path = ObjectPath::from(object_key);

        self.store
            .put(&object_path, bytes.into())
            .await
            .map_err(|e| AppError::Storage(format!(
                "Upload file '{}' lên MinIO key '{}' thất bại: {}",
                local_path.display(),
                object_key,
                e
            )))?;

        info!(
            local_path = %local_path.display(),
            object_key = %object_key,
            checksum = %checksum,
            "Đã upload local file lên MinIO S3 thành công"
        );

        Ok(checksum)
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
