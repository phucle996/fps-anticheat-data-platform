use serde::{Deserialize, Serialize};

/// EvidenceItem đại diện cho 1 bằng chứng chi tiết về đặc trưng nghi vấn gian lận
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub struct EvidenceItem {
    pub feature: String,   // Tên đặc trưng ML
    pub value: f32,        // Giá trị thực tế của người chơi
    pub lobby_avg: f32,    // Giá trị trung bình của trận đấu
    pub z_score: f32,      // Chỉ số Robust Z-Score
    pub reason: String,     // Mô tả giải thích ngắn gọn lý do nghi vấn
}

/// EvidenceMatrix chứa danh sách các bằng chứng gian lận nổi bật nhất
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub struct EvidenceMatrix {
    pub top_evidence_features: Vec<EvidenceItem>,
}

/// EvidenceEngine tính toán Robust Z-Score và trích xuất bằng chứng gian lận cho người chơi
pub struct EvidenceEngine;

impl EvidenceEngine {
    /// Generate_evidence tính toán bằng chứng bất thường từ 6 đặc trưng Gold features
    pub fn generate_evidence(features: &[f32; 6]) -> EvidenceMatrix {
        let feature_names = [
            "kills_per_minute",
            "damage_per_minute",
            "headshot_ratio",
            "damage_per_kill",
            "movement_per_minute",
            "performance_versus_lobby",
        ];

        // Giá trị Trung vị (Median) tham chiếu chuẩn của Lobby
        let lobby_medians = [0.15, 120.0, 0.18, 100.0, 150.0, 200.0];
        // Độ lệch tuyệt đối trung vị (MAD) tham chiếu của Lobby
        let lobby_mads = [0.08, 45.0, 0.08, 30.0, 40.0, 80.0];

        let mut items = Vec::new();

        for i in 0..6 {
            let val = features[i];
            let median = lobby_medians[i];
            let mad = lobby_mads[i];

            // Robust Z-Score = (X - Median) / (MAD * 1.4826)
            let z_score = (val - median) / (mad * 1.4826);

            if z_score > 1.5 {
                let reason = format!(
                    "Chỉ số {} ({:.2}) vượt quá bất thường so với trung bình trận ({:.2}) với Robust Z-Score +{:.1}",
                    feature_names[i], val, median, z_score
                );

                items.push(EvidenceItem {
                    feature: feature_names[i].to_string(),
                    value: val,
                    lobby_avg: median,
                    z_score: (z_score * 10.0).round() / 10.0,
                    reason,
                });
            }
        }

        // Sắp xếp các đặc trưng theo Robust Z-Score giảm dần và lấy Top 2
        items.sort_by(|a, b| b.z_score.partial_cmp(&a.z_score).unwrap_or(std::cmp::Ordering::Equal));
        items.truncate(2);

        EvidenceMatrix {
            top_evidence_features: items,
        }
    }
}
