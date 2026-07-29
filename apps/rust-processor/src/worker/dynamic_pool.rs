use super::circuit_breaker::{CircuitBreakerState, ResourceCircuitBreaker};
use super::r_spawner::RWorkerSpawner;
use crate::storage::MinioWriter;
use std::sync::{Arc, Mutex};
use tracing::{info, warn};

/// RDynamicWorkerPool quản lý tập hợp các R Workers động dựa trên Pure Resource Utilization & Circuit Breaker
#[derive(Clone)]
pub struct RDynamicWorkerPool {
    spawner: RWorkerSpawner,                           // Bộ điều phối async subprocess spawner
    circuit_breaker: Arc<Mutex<ResourceCircuitBreaker>>, // Circuit Breaker bảo vệ CPU/RAM
}

impl RDynamicWorkerPool {
    /// New khởi tạo RDynamicWorkerPool với Pure Resource-Driven Auto-Scaling
    /// Nhận MinioWriter để spawner có thể download manifest/Parquet từ MinIO trước khi gọi Rscript
    pub fn new(max_capacity: usize, writer: Arc<MinioWriter>) -> Self {
        let capacity = if max_capacity == 0 { 64 } else { max_capacity };
        info!(
            max_capacity = capacity,
            "Đã khởi tạo Pure Resource-Driven RDynamicWorkerPool (CPU/RAM Circuit Breaker Active)"
        );

        Self {
            spawner: RWorkerSpawner::new(capacity, writer),
            circuit_breaker: Arc::new(Mutex::new(ResourceCircuitBreaker::default_limits())),
        }
    }

    /// Dispatch_manifest điều phối batch manifest tới R Worker nếu Circuit Breaker ở trạng thái CLOSED
    /// manifest_key là S3 object key (không phải local path) — spawner sẽ download trước khi xử lý
    pub fn dispatch_manifest(&self, manifest_key: String) {
        let mut cb = match self.circuit_breaker.lock() {
            Ok(guard) => guard,
            Err(poisoned) => poisoned.into_inner(),
        };

        if cb.can_spawn() {
            info!(
                manifest_key = %manifest_key,
                "Dispatching manifest task tới Pure Resource-Driven R Worker Pool..."
            );
            self.spawner.spawn_worker(manifest_key);
        } else {
            warn!(
                manifest_key = %manifest_key,
                circuit_state = ?cb.state(),
                "Circuit Breaker đang OPEN (CPU >= 80% hoặc RAM >= 85%)! Tạm hoãn dispatch R Worker để bảo vệ hệ thống"
            );
        }
    }

    /// Circuit_breaker_state trả về trạng thái Circuit Breaker hiện tại
    pub fn circuit_breaker_state(&self) -> CircuitBreakerState {
        let cb = match self.circuit_breaker.lock() {
            Ok(guard) => guard,
            Err(poisoned) => poisoned.into_inner(),
        };
        cb.state()
    }
}
