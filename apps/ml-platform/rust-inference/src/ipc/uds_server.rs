use crate::decision::{DecisionEvaluator, DecisionOutcome};
use crate::error::{AppError, Result};
use crate::evidence::{EvidenceEngine, EvidenceMatrix};
use crate::inference::OnnxInferenceEngine;
use serde::{Deserialize, Serialize};
use std::future::Future;
use std::os::unix::fs::{FileTypeExt, PermissionsExt};
use std::path::Path;
use std::sync::Arc;
use std::time::Duration;
use tokio::io::{AsyncBufReadExt, AsyncReadExt, AsyncWriteExt, BufReader};
use tokio::net::{UnixListener, UnixStream};
use tokio::sync::Semaphore;
use tokio::time::timeout;
use tracing::{info, warn};

fn default_op() -> String {
    "predict".to_string()
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct IpcPredictRequest {
    #[serde(default = "default_op")]
    pub op: String,
    pub match_id: String,
    pub player_id: String,
    pub features: Vec<f32>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct IpcPredictResponse {
    pub status: String,
    pub match_id: String,
    pub player_id: String,
    pub risk_score: f32,
    pub risk_level: String,
    pub model_version: String,
    pub evidence_matrix: EvidenceMatrix,
    pub decision_outcome: Option<DecisionOutcome>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub error: Option<String>,
}

pub struct UdsIpcServer {
    socket_path: String,
    engine: OnnxInferenceEngine,
    evaluator: DecisionEvaluator,
    concurrency: Arc<Semaphore>,
    max_request_bytes: usize,
}

impl UdsIpcServer {
    pub fn new(
        socket_path: String,
        engine: OnnxInferenceEngine,
        policy_path: &str,
        max_concurrency: usize,
        max_request_bytes: usize,
    ) -> Result<Self> {
        if max_concurrency == 0 || max_request_bytes == 0 {
            return Err(AppError::Config(
                "IPC concurrency và request limit phải lớn hơn 0".to_string(),
            ));
        }
        Ok(Self {
            socket_path,
            engine,
            evaluator: DecisionEvaluator::new(policy_path)?,
            concurrency: Arc::new(Semaphore::new(max_concurrency)),
            max_request_bytes,
        })
    }

    pub async fn run(&self) -> Result<()> {
        self.run_until_shutdown(std::future::pending::<()>()).await
    }

    pub async fn run_until_shutdown<F>(&self, shutdown: F) -> Result<()>
    where
        F: Future<Output = ()>,
    {
        self.prepare_socket_path()?;
        let listener = UnixListener::bind(&self.socket_path).map_err(|err| {
            AppError::Ipc(format!(
                "Không thể bind Unix Domain Socket '{}': {err}",
                self.socket_path
            ))
        })?;
        std::fs::set_permissions(&self.socket_path, std::fs::Permissions::from_mode(0o660))?;

        info!(
            socket_path = %self.socket_path,
            max_request_bytes = self.max_request_bytes,
            "UDS inference server sẵn sàng"
        );

        tokio::pin!(shutdown);
        loop {
            tokio::select! {
                _ = &mut shutdown => {
                    info!("UDS server nhận graceful shutdown");
                    break;
                }
                accepted = listener.accept() => {
                    match accepted {
                        Ok((stream, _)) => {
                            // Permit được chờ trước khi spawn để task count và memory có bound rõ ràng.
                            let permit = self.concurrency.clone().acquire_owned().await.map_err(|_| {
                                AppError::Ipc("IPC concurrency limiter đã đóng".to_string())
                            })?;
                            let engine = self.engine.clone();
                            let evaluator = self.evaluator.clone();
                            let max_request_bytes = self.max_request_bytes;
                            tokio::spawn(async move {
                                let _permit = permit;
                                if let Err(err) = handle_connection(
                                    stream,
                                    engine,
                                    evaluator,
                                    max_request_bytes,
                                ).await {
                                    warn!(error = %err, "IPC request thất bại");
                                }
                            });
                        }
                        Err(err) => warn!(error = %err, "Lỗi accept Unix Domain Socket"),
                    }
                }
            }
        }

        drop(listener);
        if Path::new(&self.socket_path).exists() {
            std::fs::remove_file(&self.socket_path)?;
        }
        Ok(())
    }

    fn prepare_socket_path(&self) -> Result<()> {
        let path = Path::new(&self.socket_path);
        if !path.exists() {
            return Ok(());
        }
        let metadata = std::fs::symlink_metadata(path)?;
        if !metadata.file_type().is_socket() {
            return Err(AppError::Ipc(format!(
                "Từ chối ghi đè non-socket path '{}'",
                self.socket_path
            )));
        }
        std::fs::remove_file(path)?;
        Ok(())
    }
}

async fn handle_connection(
    stream: UnixStream,
    engine: OnnxInferenceEngine,
    evaluator: DecisionEvaluator,
    max_request_bytes: usize,
) -> Result<()> {
    let reader = BufReader::new(stream);
    let mut limited_reader = reader.take((max_request_bytes + 1) as u64);
    let mut payload = Vec::new();
    let bytes_read = timeout(
        Duration::from_secs(2),
        limited_reader.read_until(b'\n', &mut payload),
    )
    .await
    .map_err(|_| AppError::Ipc("Timeout khi đọc IPC request".to_string()))??;
    if bytes_read == 0 {
        return Err(AppError::Ipc("IPC request rỗng".to_string()));
    }

    let mut stream = limited_reader.into_inner().into_inner();
    let response = if payload.len() > max_request_bytes {
        error_response("", "", "IPC request vượt giới hạn kích thước")
    } else {
        match serde_json::from_slice::<IpcPredictRequest>(&payload) {
            Ok(request) => evaluate_request(request, &engine, &evaluator),
            Err(_) => error_response("", "", "JSON IPC request không hợp lệ"),
        }
    };
    let mut response_bytes = serde_json::to_vec(&response)?;
    response_bytes.push(b'\n');
    timeout(Duration::from_secs(2), stream.write_all(&response_bytes))
        .await
        .map_err(|_| AppError::Ipc("Timeout khi ghi IPC response".to_string()))??;
    Ok(())
}

fn evaluate_request(
    request: IpcPredictRequest,
    engine: &OnnxInferenceEngine,
    evaluator: &DecisionEvaluator,
) -> IpcPredictResponse {
    if request.op != "predict"
        || request.match_id.trim().is_empty()
        || request.player_id.trim().is_empty()
        || request.match_id.len() > 256
        || request.player_id.len() > 256
    {
        return error_response(
            &request.match_id,
            &request.player_id,
            "op hoặc identifier không hợp lệ",
        );
    }

    match engine.predict(&request.features) {
        Ok((risk_score, risk_level)) => {
            let evidence_matrix = EvidenceEngine::generate_evidence(&request.features);
            let decision_outcome =
                evaluator.evaluate(risk_score, &evidence_matrix, &request.features);
            IpcPredictResponse {
                status: "ok".to_string(),
                match_id: request.match_id,
                player_id: request.player_id,
                risk_score,
                risk_level,
                model_version: engine.version(),
                evidence_matrix,
                decision_outcome: Some(decision_outcome),
                error: None,
            }
        }
        Err(err) => error_response(&request.match_id, &request.player_id, &err.to_string()),
    }
}

fn error_response(match_id: &str, player_id: &str, message: &str) -> IpcPredictResponse {
    IpcPredictResponse {
        status: "UNAVAILABLE".to_string(),
        match_id: match_id.to_string(),
        player_id: player_id.to_string(),
        risk_score: 0.0,
        risk_level: "UNAVAILABLE".to_string(),
        model_version: "UNAVAILABLE".to_string(),
        evidence_matrix: EvidenceMatrix {
            top_evidence_features: Vec::new(),
        },
        decision_outcome: None,
        error: Some(message.to_string()),
    }
}
