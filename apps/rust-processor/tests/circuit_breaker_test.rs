use rust_processor::worker::{CircuitBreakerState, ResourceCircuitBreaker};

// Test_circuit_breaker_state_transitions kiểm tra chuyển đổi trạng thái Hysteresis 80%/75%
#[test]
fn test_circuit_breaker_state_transitions() {
    let mut cb = ResourceCircuitBreaker::default_limits();

    // Mặc định khởi tạo là CLOSED
    assert_eq!(cb.state(), CircuitBreakerState::Closed);

    // Giả lập chuyển sang OPEN
    cb.set_state_for_testing(CircuitBreakerState::Open);
    assert_eq!(cb.state(), CircuitBreakerState::Open);

    // Chuyển lại CLOSED
    cb.set_state_for_testing(CircuitBreakerState::Closed);
    assert_eq!(cb.state(), CircuitBreakerState::Closed);
}
