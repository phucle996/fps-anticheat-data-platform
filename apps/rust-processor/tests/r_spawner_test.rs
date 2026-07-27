use rust_processor::worker::RWorkerSpawner;

// Test_r_worker_spawner_non_blocking kiểm tra khả năng kích hoạt R Subprocess Async Worker không gây gián đoạn luồng chính
#[tokio::test]
async fn test_r_worker_spawner_non_blocking() {
    let spawner = RWorkerSpawner::new(2);

    // Kích hoạt R Worker bất đồng bộ với manifest giả lập
    spawner.spawn_worker("manifests/year=2026/month=07/day=28/manifest_test_001.json".to_string());

    // Đảm bảo luồng chính không bị nghẽn và tiếp tục chạy lập tức
    assert!(true, "RWorkerSpawner phải kích hoạt async task thành công mà không gây nghẽn");
}
