use std::fs;
use tracing::{info, warn};

/// CircuitBreakerState biểu diễn 2 trạng thái của bộ ngắt mạch tài nguyên
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum CircuitBreakerState {
    Closed, // Bình thường: Cho phép spawn/dispatch R Workers
    Open,   // Tạm dừng: Quá tải CPU/RAM, ngắt spawn để bảo vệ luồng Ingestion thô
}

/// ResourceCircuitBreaker quản lý trễ Hysteresis Gap (80% High / 75% Low) đo lường tài nguyên Linux procfs real-time
pub struct ResourceCircuitBreaker {
    state: CircuitBreakerState, // Trạng thái hiện tại
    high_cpu_watermark: f32,    // Ngưỡng trần CPU (80.0%)
    low_cpu_watermark: f32,     // Ngưỡng khôi phục CPU (75.0%)
    high_mem_watermark: f32,    // Ngưỡng trần RAM (85.0%)
    low_mem_watermark: f32,     // Ngưỡng khôi phục RAM (80.0%)
    prev_idle: u64,             // Idle CPU ticks trước đó
    prev_total: u64,            // Total CPU ticks trước đó
}

impl ResourceCircuitBreaker {
    /// New khởi tạo ResourceCircuitBreaker với các ngưỡng tùy biến
    pub fn new(high_cpu: f32, low_cpu: f32, high_mem: f32, low_mem: f32) -> Self {
        Self {
            state: CircuitBreakerState::Closed,
            high_cpu_watermark: high_cpu,
            low_cpu_watermark: low_cpu,
            high_mem_watermark: high_mem,
            low_mem_watermark: low_mem,
            prev_idle: 0,
            prev_total: 0,
        }
    }

    /// Default_limits khởi tạo với các ngưỡng chuẩn Cloud-Native HA (80% High / 75% Low)
    pub fn default_limits() -> Self {
        Self::new(80.0, 75.0, 85.0, 80.0)
    }

    /// State trả về trạng thái CircuitBreakerState hiện tại
    pub fn state(&self) -> CircuitBreakerState {
        self.state
    }

    /// Can_spawn kiểm tra tài nguyên real-time và quyết định cho phép spawn R Worker hay tạm dừng
    pub fn can_spawn(&mut self) -> bool {
        let (global_cpu, mem_usage) = self.read_proc_metrics();

        match self.state {
            CircuitBreakerState::Closed => {
                // TRIP TO OPEN: Khi CPU >= 80% hoặc RAM >= 85%
                if global_cpu >= self.high_cpu_watermark || mem_usage >= self.high_mem_watermark {
                    self.state = CircuitBreakerState::Open;
                    warn!(
                        cpu_usage = %format!("{:.1}%", global_cpu),
                        mem_usage = %format!("{:.1}%", mem_usage),
                        high_cpu_limit = %format!("{:.1}%", self.high_cpu_watermark),
                        "Resource Circuit Breaker TRIPPED to OPEN state! Tạm dừng spawn R Worker mới"
                    );
                    false
                } else {
                    true
                }
            }
            CircuitBreakerState::Open => {
                // RECOVER TO CLOSED: Chỉ khi CPU <= 75% và RAM <= 80% (Hysteresis Gap)
                if global_cpu <= self.low_cpu_watermark && mem_usage <= self.low_mem_watermark {
                    self.state = CircuitBreakerState::Closed;
                    info!(
                        cpu_usage = %format!("{:.1}%", global_cpu),
                        mem_usage = %format!("{:.1}%", mem_usage),
                        low_cpu_limit = %format!("{:.1}%", self.low_cpu_watermark),
                        "Resource Circuit Breaker RECOVERED to CLOSED state! Cho phép spawn R Worker trở lại"
                    );
                    true
                } else {
                    false
                }
            }
        }
    }

    /// Read_proc_metrics đọc trực tiếp chỉ số CPU % và RAM % từ Linux /proc/stat và /proc/meminfo
    fn read_proc_metrics(&mut self) -> (f32, f32) {
        // Đọc % CPU từ /proc/stat
        let cpu_usage = if let Ok(stat_str) = fs::read_to_string("/proc/stat") {
            if let Some(first_line) = stat_str.lines().next() {
                let parts: Vec<&str> = first_line.split_whitespace().collect();
                if parts.len() >= 5 && parts[0] == "cpu" {
                    let user: u64 = parts[1].parse().unwrap_or(0);
                    let nice: u64 = parts[2].parse().unwrap_or(0);
                    let system: u64 = parts[3].parse().unwrap_or(0);
                    let idle: u64 = parts[4].parse().unwrap_or(0);
                    let iowait: u64 = parts.get(5).and_then(|p| p.parse().ok()).unwrap_or(0);
                    let irq: u64 = parts.get(6).and_then(|p| p.parse().ok()).unwrap_or(0);
                    let softirq: u64 = parts.get(7).and_then(|p| p.parse().ok()).unwrap_or(0);
                    let steal: u64 = parts.get(8).and_then(|p| p.parse().ok()).unwrap_or(0);

                    let idle_time = idle + iowait;
                    let total_time = user + nice + system + idle + iowait + irq + softirq + steal;

                    let diff_idle = idle_time.saturating_sub(self.prev_idle);
                    let diff_total = total_time.saturating_sub(self.prev_total);

                    self.prev_idle = idle_time;
                    self.prev_total = total_time;

                    if diff_total > 0 {
                        ((diff_total - diff_idle) as f32 / diff_total as f32) * 100.0
                    } else {
                        0.0
                    }
                } else {
                    0.0
                }
            } else {
                0.0
            }
        } else {
            0.0
        };

        // Đọc % RAM từ /proc/meminfo
        let mem_usage = if let Ok(mem_str) = fs::read_to_string("/proc/meminfo") {
            let mut total_kb: f32 = 0.0;
            let mut available_kb: f32 = 0.0;
            for line in mem_str.lines() {
                if line.starts_with("MemTotal:") {
                    if let Some(val) = line.split_whitespace().nth(1) {
                        total_kb = val.parse().unwrap_or(0.0);
                    }
                } else if line.starts_with("MemAvailable:") {
                    if let Some(val) = line.split_whitespace().nth(1) {
                        available_kb = val.parse().unwrap_or(0.0);
                    }
                }
            }
            if total_kb > 0.0 {
                ((total_kb - available_kb) / total_kb) * 100.0
            } else {
                0.0
            }
        } else {
            0.0
        };

        (cpu_usage, mem_usage)
    }

    /// Set_state_for_testing hỗ trợ giả lập trạng thái cho unit tests
    pub fn set_state_for_testing(&mut self, state: CircuitBreakerState) {
        self.state = state;
    }
}
