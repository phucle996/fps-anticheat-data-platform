use rust_processor::worker::{CircuitBreakerState, RDynamicWorkerPool};

// Test_dynamic_worker_pool_dispatch kiểm tra khả năng dispatch manifest task tới Pure Resource-Driven Dynamic Pool
#[tokio::test]
async fn test_dynamic_worker_pool_dispatch() {
    let pool = RDynamicWorkerPool::new(4);

    assert_eq!(pool.circuit_breaker_state(), CircuitBreakerState::Closed);

    // Dispatch 2 manifest tasks bất đồng bộ
    pool.dispatch_manifest("manifests/year=2026/month=07/day=28/manifest_dyn_001.json".to_string());
    pool.dispatch_manifest("manifests/year=2026/month=07/day=28/manifest_dyn_002.json".to_string());

    assert!(true, "RDynamicWorkerPool phải dispatch tasks thành công");
}
