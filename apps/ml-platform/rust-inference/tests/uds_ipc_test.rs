mod common;

use rust_inference::inference::OnnxInferenceEngine;
use rust_inference::ipc::{IpcPredictRequest, IpcPredictResponse, UdsIpcServer};
use tokio::io::{AsyncBufReadExt, AsyncWriteExt, BufReader};
use tokio::net::UnixStream;

#[tokio::test]
async fn test_uds_ipc_stream() {
    let temp_dir = tempfile::tempdir().unwrap();
    common::write_test_bundle(temp_dir.path());
    let socket_dir = tempfile::tempdir().unwrap();
    let socket_path = socket_dir.path().join("inference.sock");
    let engine = OnnxInferenceEngine::new(temp_dir.path().to_str().unwrap()).unwrap();
    let server = UdsIpcServer::new(
        socket_path.to_string_lossy().into_owned(),
        engine,
        &common::policy_path(),
        4,
        16 * 1024,
    )
    .unwrap();

    let server_task = tokio::spawn(async move {
        let _ = server.run().await;
    });
    tokio::time::sleep(tokio::time::Duration::from_millis(50)).await;

    let mut client = UnixStream::connect(&socket_path).await.unwrap();
    let req = IpcPredictRequest {
        op: "predict".to_string(),
        match_id: "match_100".to_string(),
        player_id: "player_A".to_string(),
        features: vec![8.0, 1.0, 1.0, 5.0, 4.0],
    };
    let mut req_bytes = serde_json::to_vec(&req).unwrap();
    req_bytes.push(b'\n');
    client.write_all(&req_bytes).await.unwrap();

    let mut response = String::new();
    BufReader::new(client)
        .read_line(&mut response)
        .await
        .unwrap();
    let resp: IpcPredictResponse = serde_json::from_str(&response).unwrap();
    assert_eq!(resp.status, "ok");
    assert_eq!(resp.model_version, "test-v1");
    assert_eq!(resp.risk_level, "CRITICAL");

    server_task.abort();
}
