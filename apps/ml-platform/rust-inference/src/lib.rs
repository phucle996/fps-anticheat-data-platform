pub mod config;
pub mod error;
pub mod evidence;
pub mod inference;
pub mod ipc;
pub mod storage;

pub use config::Config;
pub use error::{AppError, Result};
pub use evidence::{EvidenceEngine, EvidenceItem, EvidenceMatrix};
pub use inference::{LoadedModel, OnnxInferenceEngine};
pub use ipc::{IpcPredictRequest, IpcPredictResponse, UdsIpcServer};
pub use storage::{PredictionParquetWriter, PredictionRecord};
