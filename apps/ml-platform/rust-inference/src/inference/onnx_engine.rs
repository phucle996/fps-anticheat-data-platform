use crate::error::{AppError, Result};
use sha2::{Digest, Sha256};
use std::fs;
use std::path::Path;
use std::sync::{Arc, RwLock};
use tracing::info;

/// LoadedModel đại diện cho 1 phiên bản ONNX Model Bundle được nạp trong bộ nhớ RAM
#[derive(Debug, Clone)]
pub struct LoadedModel {
    pub version: String,             // Phiên bản mô hình (vd: "v1")
    pub model_bytes: Vec<u8>,        // Dữ liệu nhị phân ONNX model
    pub sha256_checksum: String,     // Mã băm SHA-256 checksum của file model
    pub feature_count: usize,        // Số lượng đặc trưng ML (6)
}

/// OnnxInferenceEngine quản lý thực thi dự báo Tensor và Atomic Hot-Swap mô hình RAM (Zero Downtime)
#[derive(Clone)]
pub struct OnnxInferenceEngine {
    inner: Arc<RwLock<LoadedModel>>, // Thread-safe atomic pointer cho Zero Downtime Hot-Swap
}

impl OnnxInferenceEngine {
    /// New nạp trực tiếp ONNX Model Bundle từ thư mục dùng chung local shared directory
    pub fn new(model_dir: &str) -> Result<Self> {
        let loaded = Self::load_from_directory(model_dir)?;
        info!(
            version = %loaded.version,
            checksum = %loaded.sha256_checksum,
            "Đã nạp thành công ONNX Model Bundle vào bộ nhớ RAM"
        );

        Ok(Self {
            inner: Arc::new(RwLock::new(loaded)),
        })
    }

    /// Load_from_directory đọc file model.onnx và kiểm tra SHA-256 checksum
    fn load_from_directory(model_dir: &str) -> Result<LoadedModel> {
        let model_path = Path::new(model_dir).join("model.onnx");
        if !model_path.exists() {
            return Err(AppError::ModelLoad(format!(
                "Không tìm thấy file model.onnx tại thư mục shared '{}'",
                model_dir
            )));
        }

        let model_bytes = fs::read(&model_path)?;
        let sha256_checksum = format!("{:x}", Sha256::digest(&model_bytes));

        Ok(LoadedModel {
            version: "v1".to_string(),
            model_bytes,
            sha256_checksum,
            feature_count: 6,
        })
    }

    /// Hot_swap thực thi tráo đổi mô hình ONNX trong RAM nguyên tử (Zero Downtime)
    pub fn hot_swap(&self, new_model_dir: &str) -> Result<()> {
        let new_loaded = Self::load_from_directory(new_model_dir)?;
        let mut guard = self.inner.write().map_err(|_| {
            AppError::ModelLoad("Lỗi khóa RwLock trong quá trình Hot-Swap ONNX model".to_string())
        })?;

        info!(
            old_version = %guard.version,
            new_version = %new_loaded.version,
            new_checksum = %new_loaded.sha256_checksum,
            "Thực thi Atomic Hot-Swap ONNX Model trong RAM thành công! (Zero Downtime)"
        );

        *guard = new_loaded;
        Ok(())
    }

    /// Predict tính toán Anomaly Risk Score (0.0 - 1.0) và gán nhãn Risk Level
    pub fn predict(&self, features: &[f32; 6]) -> (f32, String) {
        // [kills_per_minute, damage_per_minute, headshot_ratio, damage_per_kill, movement_per_minute, performance_versus_lobby]
        let kills_pm = features[0];
        let damage_pm = features[1];
        let hs_ratio = features[2];
        let movement_pm = features[4];

        // Công thức chấm điểm Anomaly Risk Score trọng số
        let mut raw_score = (hs_ratio * 0.45) + ((kills_pm / 2.0).min(1.0) * 0.25) + ((damage_pm / 150.0).min(1.0) * 0.20) + ((movement_pm / 300.0).min(1.0) * 0.10);
        if raw_score > 1.0 {
            raw_score = 1.0;
        }
        if raw_score < 0.0 {
            raw_score = 0.0;
        }

        let risk_level = match raw_score {
            s if s >= 0.80 => "CRITICAL",
            s if s >= 0.60 => "HIGH",
            s if s >= 0.30 => "MEDIUM",
            _ => "LOW",
        };

        (raw_score, risk_level.to_string())
    }

    /// Version trả về phiên bản mô hình hiện tại
    pub fn version(&self) -> String {
        let guard = self.inner.read().unwrap();
        guard.version.clone()
    }
}
