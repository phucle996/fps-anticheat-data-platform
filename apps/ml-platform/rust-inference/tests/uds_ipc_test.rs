use rust_inference::inference::OnnxInferenceEngine;
use rust_inference::ipc::{IpcPredictRequest, IpcPredictResponse, UdsIpcServer};
use std::fs;
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::UnixStream;

// Test_uds_ipc_stream kiểm tra luồng truyền nhận Unix Domain Socket IPC giữa Client và Server
#[tokio::test]
async fn test_uds_ipc_stream() {
    let temp_dir = std::env::temp_dir().join("test_model_dir_ipc");
    let _ = fs::create_dir_all(&temp_dir);
    let model_file = temp_dir.join("model.onnx");
    let _ = fs::write(&model_file, b"ONNX_TEST_MODEL_BYTES_IPC");

    let socket_path = format!("{}/test_ipc.sock", std::env::temp_dir().display());
    let engine = OnnxInferenceEngine::new(temp_dir.to_str().unwrap()).unwrap();
    let server = UdsIpcServer::new(socket_path.clone(), engine);

    // Khởi chạy server trong background tokio task
    tokio::spawn(async move {
        let _ = server.run().await;
    });

    // Chờ 50ms cho server bind socket
    tokio::time::sleep(tokio::time::Duration::from_millis(50)).await;

    // Giả lập client Go API kết nối Unix Domain Socket
    let mut client = UnixStream::connect(&socket_path).await.unwrap();
    let req = IpcPredictRequest {
        op: "predict".to_string(),
        match_id: "match_100".to_string(),
        player_id: "player_A".to_string(),
        features: [1.50, 140.0, 0.95, 120.0, 250.0, 800.0],
    };

    let req_bytes = serde_json::to_vec(&req).unwrap();
    client.write_all(&req_bytes).await.unwrap();

    let mut response_buf = [0u8; 1024];
    let bytes_read = client.read(&mut response_buf).await.unwrap();
    assert!(bytes_read > 0);

    let resp: IpcPredictResponse = serde_json::from_slice(&response_buf[..bytes_read]).unwrap();
    assert_eq!(resp.status, "ok");
    assert_eq!(resp.match_id, "match_100");
    assert_eq!(resp.player_id, "player_A");
    assert_eq!(resp.risk_level, "CRITICAL");

    let _ = fs::remove_file(socket_path);
    let _ = fs::remove_dir_all(temp_dir);
}
