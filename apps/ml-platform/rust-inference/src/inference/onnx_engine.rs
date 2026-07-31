use crate::error::{AppError, Result};
use ort::session::{builder::GraphOptimizationLevel, Session};
use ort::value::{DynValue, TensorRef};
use serde::Deserialize;
use sha2::{Digest, Sha256};
use std::collections::HashMap;
use std::fs;
use std::path::Path;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::{Arc, Mutex, RwLock};
use tracing::{info, warn};

const MODEL_FILE: &str = "model.onnx";
const FEATURE_SCHEMA_FILE: &str = "feature_schema.json";
const THRESHOLD_POLICY_FILE: &str = "threshold_policy.json";
const CHECKSUM_FILE: &str = "checksums.sha256";

#[derive(Debug, Deserialize)]
struct FeatureSchema {
    model_version: String,
    features: Vec<String>,
}

#[derive(Debug, Deserialize)]
struct ThresholdPolicy {
    thresholds: HashMap<String, f32>,
}

#[derive(Debug, Clone)]
struct RiskThresholds {
    medium: f32,
    high: f32,
    critical: f32,
}

/// LoadedModel giữ metadata immutable và một pool ONNX sessions.
///
/// Mỗi Session cần mutable borrow khi chạy; pool tránh biến một mutex duy nhất
/// thành điểm nghẽn trong khi vẫn cho phép hot-swap pointer nguyên tử.
pub struct LoadedModel {
    pub version: String,
    pub sha256_checksum: String,
    pub feature_names: Vec<String>,
    sessions: Vec<Mutex<Session>>,
    next_session: AtomicUsize,
    thresholds: RiskThresholds,
}

impl std::fmt::Debug for LoadedModel {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter
            .debug_struct("LoadedModel")
            .field("version", &self.version)
            .field("sha256_checksum", &self.sha256_checksum)
            .field("feature_names", &self.feature_names)
            .field("session_pool_size", &self.sessions.len())
            .field("thresholds", &self.thresholds)
            .finish()
    }
}

/// OnnxInferenceEngine thực thi ONNX thật và chỉ tráo model sau khi toàn bundle
/// đã qua checksum, schema và ONNX Runtime validation.
#[derive(Clone)]
pub struct OnnxInferenceEngine {
    inner: Arc<RwLock<Option<Arc<LoadedModel>>>>,
    session_pool_size: usize,
}

impl OnnxInferenceEngine {
    pub fn new(model_dir: &str) -> Result<Self> {
        Self::new_with_pool(model_dir, 1)
    }

    pub fn new_with_pool(model_dir: &str, session_pool_size: usize) -> Result<Self> {
        if session_pool_size == 0 {
            return Err(AppError::Config(
                "INFERENCE_SESSION_POOL_SIZE phải lớn hơn 0".to_string(),
            ));
        }

        let loaded = Self::load_from_directory(model_dir, session_pool_size)?;
        if let Some(model) = &loaded {
            info!(
                version = %model.version,
                checksum = %model.sha256_checksum,
                session_pool_size,
                "Đã nạp và xác minh ONNX model bundle"
            );
        } else {
            warn!(
                model_dir,
                "Chưa có model bundle; inference giữ trạng thái UNAVAILABLE cho tới hot-swap"
            );
        }

        Ok(Self {
            inner: Arc::new(RwLock::new(loaded.map(Arc::new))),
            session_pool_size,
        })
    }

    pub fn is_available(&self) -> bool {
        self.inner
            .read()
            .map(|guard| guard.is_some())
            .unwrap_or(false)
    }

    pub fn version(&self) -> String {
        self.current_model()
            .map(|model| model.version.clone())
            .unwrap_or_else(|_| "UNAVAILABLE".to_string())
    }

    pub fn feature_count(&self) -> Option<usize> {
        self.current_model()
            .ok()
            .map(|model| model.feature_names.len())
    }

    /// Hot-swap chỉ giữ write lock ở bước thay Arc; load model có thể chậm được
    /// thực hiện trước lock nên request đang chạy không bị dừng theo thời gian I/O.
    pub fn hot_swap(&self, new_model_dir: &str) -> Result<bool> {
        let new_model = Self::load_from_directory(new_model_dir, self.session_pool_size)?
            .ok_or_else(|| AppError::ModelLoad("Model bundle mới chưa tồn tại".to_string()))?;

        if self
            .current_model()
            .map(|current| current.sha256_checksum == new_model.sha256_checksum)
            .unwrap_or(false)
        {
            return Ok(false);
        }

        let new_model = Arc::new(new_model);
        let mut guard = self.inner.write().map_err(|_| {
            AppError::ModelLoad("RwLock model bị poison trong hot-swap".to_string())
        })?;
        let old_version = guard
            .as_ref()
            .map(|model| model.version.as_str())
            .unwrap_or("UNAVAILABLE");
        info!(
            old_version,
            new_version = %new_model.version,
            checksum = %new_model.sha256_checksum,
            "Atomic hot-swap ONNX model thành công"
        );
        *guard = Some(new_model);
        Ok(true)
    }

    pub fn predict(&self, features: &[f32]) -> Result<(f32, String)> {
        let model = self.current_model()?;
        if features.len() != model.feature_names.len() {
            return Err(AppError::Inference(format!(
                "Sai feature count: nhận {}, model yêu cầu {} ({})",
                features.len(),
                model.feature_names.len(),
                model.feature_names.join(", ")
            )));
        }
        if features.iter().any(|value| !value.is_finite()) {
            return Err(AppError::Inference(
                "Feature chứa NaN hoặc Infinity".to_string(),
            ));
        }

        let slot = model.next_session.fetch_add(1, Ordering::Relaxed) % model.sessions.len();
        let mut session = model.sessions[slot]
            .lock()
            .map_err(|_| AppError::Inference("ONNX session mutex bị poison".to_string()))?;
        let tensor = TensorRef::from_array_view(([1usize, features.len()], features))
            .map_err(|err| AppError::Inference(format!("Tạo input tensor thất bại: {err}")))?;
        let outputs = session
            .run(ort::inputs![tensor])
            .map_err(|err| AppError::Inference(format!("ONNX Runtime thất bại: {err}")))?;

        let score = if let Some(output) = outputs.get("probabilities") {
            extract_probability(output)?
        } else {
            let mut probability = None;
            for (_, output) in outputs.iter() {
                if let Ok(value) = extract_probability(&output) {
                    probability = Some(value);
                    break;
                }
            }
            probability.ok_or_else(|| {
                AppError::Inference("Model không trả probability tensor float32".to_string())
            })?
        };
        if !score.is_finite() || !(0.0..=1.0).contains(&score) {
            return Err(AppError::Inference(format!(
                "Model trả risk probability ngoài [0,1]: {score}"
            )));
        }

        let risk_level = if score >= model.thresholds.critical {
            "CRITICAL"
        } else if score >= model.thresholds.high {
            "HIGH"
        } else if score >= model.thresholds.medium {
            "MEDIUM"
        } else {
            "LOW"
        };
        Ok((score, risk_level.to_string()))
    }

    fn current_model(&self) -> Result<Arc<LoadedModel>> {
        self.inner
            .read()
            .map_err(|_| AppError::ModelLoad("RwLock model bị poison".to_string()))?
            .clone()
            .ok_or_else(|| AppError::ModelLoad("ONNX model chưa sẵn sàng".to_string()))
    }

    fn load_from_directory(
        model_dir: &str,
        session_pool_size: usize,
    ) -> Result<Option<LoadedModel>> {
        let directory = Path::new(model_dir);
        let model_path = directory.join(MODEL_FILE);
        if !model_path.exists() {
            return Ok(None);
        }

        for required in [FEATURE_SCHEMA_FILE, THRESHOLD_POLICY_FILE, CHECKSUM_FILE] {
            if !directory.join(required).is_file() {
                return Err(AppError::ModelLoad(format!(
                    "Model bundle thiếu file bắt buộc {required}"
                )));
            }
        }

        let checksums: HashMap<String, String> =
            serde_json::from_slice(&fs::read(directory.join(CHECKSUM_FILE))?)?;
        for (filename, expected) in &checksums {
            if filename == CHECKSUM_FILE {
                continue;
            }
            let artifact_path = directory.join(filename);
            if !artifact_path.is_file() {
                return Err(AppError::ChecksumMismatch(format!(
                    "Checksum manifest tham chiếu artifact không tồn tại: {filename}"
                )));
            }
            let actual = format!("{:x}", Sha256::digest(fs::read(&artifact_path)?));
            if !actual.eq_ignore_ascii_case(expected) {
                return Err(AppError::ChecksumMismatch(format!(
                    "{filename}: expected {expected}, actual {actual}"
                )));
            }
        }

        let model_checksum = checksums.get(MODEL_FILE).cloned().ok_or_else(|| {
            AppError::ChecksumMismatch("checksums.sha256 thiếu model.onnx".to_string())
        })?;
        let feature_schema: FeatureSchema =
            serde_json::from_slice(&fs::read(directory.join(FEATURE_SCHEMA_FILE))?)?;
        if feature_schema.features.is_empty() || feature_schema.features.len() > 128 {
            return Err(AppError::ModelLoad(
                "Feature schema phải chứa từ 1 đến 128 features".to_string(),
            ));
        }

        let policy: ThresholdPolicy =
            serde_json::from_slice(&fs::read(directory.join(THRESHOLD_POLICY_FILE))?)?;
        let thresholds = RiskThresholds {
            medium: required_threshold(&policy, "MEDIUM")?,
            high: required_threshold(&policy, "HIGH")?,
            critical: required_threshold(&policy, "CRITICAL")?,
        };
        if !(0.0..thresholds.medium).contains(&0.0)
            || thresholds.medium >= thresholds.high
            || thresholds.high >= thresholds.critical
            || thresholds.critical > 1.0
        {
            return Err(AppError::ModelLoad(
                "Thresholds phải tăng dần trong khoảng 0 < MEDIUM < HIGH < CRITICAL <= 1"
                    .to_string(),
            ));
        }

        let mut sessions = Vec::with_capacity(session_pool_size);
        for _ in 0..session_pool_size {
            let builder = Session::builder().map_err(|err| {
                AppError::ModelLoad(format!("Không tạo được ONNX session builder: {err}"))
            })?;
            let mut builder = builder
                .with_optimization_level(GraphOptimizationLevel::All)
                .map_err(|err| {
                    AppError::ModelLoad(format!("Không bật được ONNX optimization: {err}"))
                })?;
            let session = builder.commit_from_file(&model_path).map_err(|err| {
                AppError::ModelLoad(format!("ONNX model validation thất bại: {err}"))
            })?;
            if session.inputs().len() != 1 {
                return Err(AppError::ModelLoad(format!(
                    "Model phải có đúng 1 input tensor, nhận {}",
                    session.inputs().len()
                )));
            }
            sessions.push(Mutex::new(session));
        }

        Ok(Some(LoadedModel {
            version: feature_schema.model_version,
            sha256_checksum: model_checksum,
            feature_names: feature_schema.features,
            sessions,
            next_session: AtomicUsize::new(0),
            thresholds,
        }))
    }
}

fn extract_probability(output: &DynValue) -> Result<f32> {
    let (_, values) = output.try_extract_tensor::<f32>().map_err(|err| {
        AppError::Inference(format!(
            "Output probabilities không phải tensor float32: {err}"
        ))
    })?;
    match values {
        [] => Err(AppError::Inference("Output probabilities rỗng".to_string())),
        [single] => Ok(*single),
        multiple => Ok(*multiple.last().expect("đã kiểm tra non-empty")),
    }
}

fn required_threshold(policy: &ThresholdPolicy, name: &str) -> Result<f32> {
    policy
        .thresholds
        .get(name)
        .copied()
        .filter(|value| value.is_finite())
        .ok_or_else(|| AppError::ModelLoad(format!("Threshold policy thiếu {name} hợp lệ")))
}
