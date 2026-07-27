use super::circuit_breaker::{CircuitBreakerState, ResourceCircuitBreaker};
use super::r_spawner::RWorkerSpawner;
use std::sync::{Arc, Mutex};
use tracing::{info, warn};

/// RDynamicWorkerPool quản lý tập hợp các R Workers động dựa trên Pure Resource Utilization & Circuit Breaker
#[derive(Clone)]
pub struct RDynamicWorkerPool {
    spawner: RWorkerSpawner,                     // Bộ điều phối async subprocess spawner
    circuit_breaker: Arc<Mutex<ResourceCircuitBreaker>>, // Circuit Breaker bảo vệ CPU/RAM
}

impl RDynamicWorkerPool {
    /// New khởi tạo RDynamicWorkerPool với Pure Resource-Driven Auto-Scaling
    pub fn new(max_capacity: usize) -> Self {
        let capacity = if max_capacity == 0 { 64 } else { max_capacity };
        info!(
            max_capacity = capacity,
            "Đã khởi tạo Pure Resource-Driven RDynamicWorkerPool (CPU/RAM Circuit Breaker Active)"
        );

        Self {
            spawner: RWorkerSpawner::new(capacity),
            circuit_breaker: Arc::new(Mutex::new(ResourceCircuitBreaker::default_limits())),
        }
    }

    /// Dispatch_manifest điều phối batch manifest tới R Worker nếu Circuit Breaker ở trạng thái CLOSED
    pub fn dispatch_manifest(&self, manifest_path: String) {
        let mut cb = match self.circuit_breaker.lock() {
            Ok(guard) => guard,
            Err(poisoned) => poisoned.into_inner(),
        };

        if cb.can_spawn() {
            info!(
                manifest = %manifest_path,
                "Dispatching manifest task tới Pure Resource-Driven R Worker Pool..."
            );
            self.spawner.spawn_worker(manifest_path);
        } else {
            warn!(
                manifest = %manifest_path,
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
