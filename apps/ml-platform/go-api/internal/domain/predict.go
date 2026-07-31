package domain

// PredictRequest định nghĩa cấu trúc Yêu cầu dự báo truyền qua Unix Domain Socket IPC
type PredictRequest struct {
	Op       string    `json:"op"`        // Operation name (vd: "predict")
	MatchID  string    `json:"match_id"`  // Mã trận đấu
	PlayerID string    `json:"player_id"` // Mã người chơi
	Features []float32 `json:"features"`  // Các đặc trưng Gold Feature Contract
}

// EvidenceItem đại diện cho 1 bằng chứng chi tiết về đặc trưng nghi vấn gian lận
type EvidenceItem struct {
	Feature  string  `json:"feature"`   // Tên đặc trưng ML
	Value    float32 `json:"value"`     // Giá trị thực tế của người chơi
	LobbyAvg float32 `json:"lobby_avg"` // Giá trị trung bình của trận đấu
	ZScore   float32 `json:"z_score"`   // Chỉ số Robust Z-Score
	Reason   string  `json:"reason"`    // Mô tả giải thích ngắn gọn lý do nghi vấn
}

// EvidenceMatrix chứa danh sách các bằng chứng gian lận nổi bật nhất
type EvidenceMatrix struct {
	TopEvidenceFeatures []EvidenceItem `json:"top_evidence_features"`
}

// DecisionOutcome định nghĩa kết quả quyết định xử lý gian lận từ Decision Engine
type DecisionOutcome struct {
	Action        string `json:"action"`         // Hành động xử lý ("CLEAR", "WATCHLIST", "SUSPEND_ACCOUNT", "PERMANENT_BAN")
	Priority      string `json:"priority"`       // Mức độ ưu tiên ("LOW", "MEDIUM", "HIGH", "CRITICAL")
	Reason        string `json:"reason"`         // Lý do giải thích chi tiết
	PolicyRule    string `json:"policy_rule"`    // Quy tắc khớp điều kiện
	PolicyVersion string `json:"policy_version"` // Phiên bản policy YAML áp dụng
}

// PredictResponse định nghĩa cấu trúc Phản hồi kết quả dự báo từ Rust Inference Engine
type PredictResponse struct {
	Status          string           `json:"status"`           // Trạng thái ("ok" hoặc "error")
	MatchID         string           `json:"match_id"`         // Mã trận đấu
	PlayerID        string           `json:"player_id"`        // Mã người chơi
	RiskScore       float32          `json:"risk_score"`       // Anomaly Risk Score (0.0 - 1.0)
	RiskLevel       string           `json:"risk_level"`       // Nhãn Risk Level ("LOW", "MEDIUM", "HIGH", "CRITICAL")
	ModelVersion    string           `json:"model_version"`    // Phiên bản ONNX Model
	EvidenceMatrix  EvidenceMatrix   `json:"evidence_matrix"`  // Bằng chứng gian lận Evidence Matrix
	DecisionOutcome *DecisionOutcome `json:"decision_outcome"` // Kết quả quyết định xử lý từ Decision Engine
}
