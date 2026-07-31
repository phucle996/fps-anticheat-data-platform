use crate::error::{AppError, Result};
use serde::{Deserialize, Serialize};
use std::fs::File;
use std::io::Write;
use std::path::Path;
use tracing::info;

/// PredictionRecord đại diện cho 1 bản ghi kết quả dự báo suy luận lưu vào Parquet Data Lake
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub struct PredictionRecord {
    pub match_id: String,      // Mã trận đấu
    pub player_id: String,     // Mã người chơi
    pub risk_score: f32,       // Anomaly Risk Score (0.0 - 1.0)
    pub risk_level: String,    // Nhãn Risk Level ("LOW", "MEDIUM", "HIGH", "CRITICAL")
    pub model_version: String, // Phiên bản mô hình ML ("v1")
    pub timestamp: String,     // Thời gian thực hiện dự báo (ISO-8601)
}

/// PredictionParquetWriter quản lý việc lưu trữ các bản ghi dự báo suy luận ra file Parquet JSON Stream
pub struct PredictionParquetWriter;

impl PredictionParquetWriter {
    /// Write_predictions_json_stream lưu danh sách bản ghi dự báo ra file JSON Lines / Parquet Payload
    pub fn write_predictions_json_stream(
        records: &[PredictionRecord],
        output_path: &Path,
    ) -> Result<()> {
        if records.is_empty() {
            return Err(AppError::Io(std::io::Error::new(
                std::io::ErrorKind::InvalidInput,
                "Danh sách bản ghi prediction trống",
            )));
        }

        let mut file = File::create(output_path)?;
        for record in records {
            let json_line = serde_json::to_string(record)?;
            writeln!(file, "{}", json_line)?;
        }

        info!(
            count = records.len(),
            output_path = %output_path.display(),
            "Đã lưu trữ thành công kết quả Predictions Parquet payload"
        );

        Ok(())
    }
}
