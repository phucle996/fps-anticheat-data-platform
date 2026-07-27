use crate::error::{AppError, Result};
use crate::evidence::{EvidenceEngine, EvidenceMatrix};
use crate::inference::OnnxInferenceEngine;
use serde::{Deserialize, Serialize};
use std::fs;
use std::path::Path;
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::UnixListener;
use tracing::{info, warn};

/// IpcPredictRequest định nghĩa cấu trúc JSON IPC Yêu cầu dự báo từ Go API Gateway
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct IpcPredictRequest {
    pub op: String,                // Operation name (vd: "predict")
    pub match_id: String,          // Mã trận đấu
    pub player_id: String,         // Mã người chơi
    pub features: [f32; 6],        // 6 đặc trưng ML Gold Feature Contract
}

/// IpcPredictResponse định nghĩa cấu trúc JSON IPC Phản hồi cho Go API Gateway
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct IpcPredictResponse {
    pub status: String,                 // Trạng thái xử lý ("ok" hoặc "error")
    pub match_id: String,               // Mã trận đấu
    pub player_id: String,              // Mã người chơi
    pub risk_score: f32,                // Anomaly Risk Score (0.0 - 1.0)
    pub risk_level: String,             // Nhãn Risk Level ("LOW", "MEDIUM", "HIGH", "CRITICAL")
    pub model_version: String,          // Phiên bản ONNX Model ("v1")
    pub evidence_matrix: EvidenceMatrix, // Bằng chứng gian lận Evidence Matrix
}

/// UdsIpcServer quản lý lắng nghe và xử lý giao tiếp Unix Domain Socket IPC siêu tốc với Go API
pub struct UdsIpcServer {
    socket_path: String,           // Đường dẫn file socket (vd: "/tmp/rust_inference.sock")
    engine: OnnxInferenceEngine,   // Engine dự báo ONNX
}

impl UdsIpcServer {
    /// New khởi tạo UdsIpcServer
    pub fn new(socket_path: String, engine: OnnxInferenceEngine) -> Self {
        Self { socket_path, engine }
    }

    /// Run khởi chạy vòng lặp async tokio lắng nghe kết nối từ Go API Gateway
    pub async fn run(&self) -> Result<()> {
        if Path::new(&self.socket_path).exists() {
            let _ = fs::remove_file(&self.socket_path);
        }

        let listener = UnixListener::bind(&self.socket_path).map_err(|e| {
            AppError::Ipc(format!("Không thể bind Unix Domain Socket tại '{}': {}", self.socket_path, e))
        })?;

        info!(
            socket_path = %self.socket_path,
            "UdsIpcServer đã khởi chạy thành công! Lắng nghe IPC request từ Go API Gateway..."
        );

        loop {
            match listener.accept().await {
                Ok((mut stream, _addr)) => {
                    let engine = self.engine.clone();
                    tokio::spawn(async move {
                        let mut buffer = [0u8; 4096];
                        if let Ok(bytes_read) = stream.read(&mut buffer).await {
                            if bytes_read > 0 {
                                if let Ok(req) = serde_json::from_slice::<IpcPredictRequest>(&buffer[..bytes_read]) {
                                    let (risk_score, risk_level) = engine.predict(&req.features);
                                    let evidence_matrix = EvidenceEngine::generate_evidence(&req.features);

                                    let resp = IpcPredictResponse {
                                        status: "ok".to_string(),
                                        match_id: req.match_id,
                                        player_id: req.player_id,
                                        risk_score,
                                        risk_level,
                                        model_version: engine.version(),
                                        evidence_matrix,
                                    };

                                    if let Ok(resp_bytes) = serde_json::to_vec(&resp) {
                                        let _ = stream.write_all(&resp_bytes).await;
                                    }
                                }
                            }
                        }
                    });
                }
                Err(err) => {
                    warn!(error = %err, "Lỗi kết nối Unix Domain Socket IPC");
                }
            }
        }
    }
}
