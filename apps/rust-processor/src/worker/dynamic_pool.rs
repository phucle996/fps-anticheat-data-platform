use super::circuit_breaker::{CircuitBreakerState, ResourceCircuitBreaker};
use super::native_worker::{NativeWorkerResult, NativeWorkerSpawner};
use crate::error::{AppError, Result};
use crate::storage::MinioWriter;
use std::sync::{Arc, Mutex};
use std::time::Duration;
use tracing::info;

/// Bounded Native Rust worker pool. Khi quá tải, caller nhận lỗi để giữ nguyên Kafka offset.
#[derive(Clone)]
pub struct DynamicWorkerPool {
    spawner: NativeWorkerSpawner,
    circuit_breaker: Arc<Mutex<ResourceCircuitBreaker>>,
}

impl DynamicWorkerPool {
    pub fn new(max_capacity: usize, writer: Arc<MinioWriter>, worker_timeout: Duration) -> Self {
        let capacity = max_capacity.max(1);
        info!(max_capacity = capacity, "Khởi tạo bounded Native Rust worker pool");
        Self {
            spawner: NativeWorkerSpawner::new(capacity, writer, worker_timeout),
            circuit_breaker: Arc::new(Mutex::new(ResourceCircuitBreaker::default_limits())),
        }
    }

    pub async fn process_manifest(&self, manifest_key: &str) -> Result<NativeWorkerResult> {
        let can_spawn = {
            let mut breaker = self
                .circuit_breaker
                .lock()
                .map_err(|_| AppError::Worker("Circuit-breaker lock bị poison".to_string()))?;
            breaker.can_spawn()
        };

        if !can_spawn {
            // Không drop work khi overload: fail batch để Kafka redeliver với backoff của supervisor.
            return Err(AppError::Worker(format!(
                "Circuit breaker OPEN; giữ Kafka offset cho manifest '{}'",
                manifest_key
            )));
        }
        self.spawner.process_manifest(manifest_key).await
    }

    pub fn circuit_breaker_state(&self) -> CircuitBreakerState {
        self.circuit_breaker
            .lock()
            .map(|breaker| breaker.state())
            .unwrap_or(CircuitBreakerState::Open)
    }
}
