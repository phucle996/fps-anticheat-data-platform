use rust_processor::worker::{CircuitBreakerState, ResourceCircuitBreaker};

#[test]
fn test_resource_circuit_breaker_starts_closed() {
    let breaker = ResourceCircuitBreaker::default_limits();
    assert_eq!(breaker.state(), CircuitBreakerState::Closed);
}

#[test]
fn test_resource_circuit_breaker_exposes_open_state() {
    let mut breaker = ResourceCircuitBreaker::default_limits();
    breaker.set_state_for_testing(CircuitBreakerState::Open);
    assert_eq!(breaker.state(), CircuitBreakerState::Open);
}
